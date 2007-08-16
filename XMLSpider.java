/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package plugins.XMLSpider;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

import freenet.client.ClientMetadata;
import freenet.client.FetchContext;
import freenet.client.FetchException;
import freenet.client.FetchResult;
import freenet.client.InsertException;
import freenet.client.async.BaseClientPutter;
import freenet.client.async.ClientCallback;
import freenet.client.async.ClientGetter;
import freenet.client.async.USKCallback;
import freenet.clients.http.PageMaker;
import freenet.clients.http.ToadletContext;
import freenet.clients.http.ToadletContextClosedException;
import freenet.clients.http.filter.ContentFilter;
import freenet.clients.http.filter.FoundURICallback;
import freenet.clients.http.filter.UnsafeContentTypeException;
import freenet.keys.FreenetURI;
import freenet.keys.USK;
import freenet.node.NodeClientCore;
import freenet.node.RequestStarter;
import freenet.oldplugins.plugin.HttpPlugin;
import freenet.oldplugins.plugin.PluginManager;
import freenet.pluginmanager.FredPlugin;
import freenet.pluginmanager.FredPluginHTTP;
import freenet.pluginmanager.FredPluginHTTPAdvanced;
import freenet.pluginmanager.FredPluginThreadless;
import freenet.pluginmanager.PluginHTTPException;
import freenet.pluginmanager.PluginRespirator;
import freenet.support.HTMLNode;
import freenet.support.Logger;
import freenet.support.MultiValueTable;
import freenet.support.api.Bucket;
import freenet.support.api.HTTPRequest;

/**
 * XMLSpider. Produces index for searching words. 
 * In case the size of the index grows up a specific threshold the index is split into several subindices
 * The indexing key is the md5 hash of the word.
 * 
 *  @author swati goyal
 *  
 */
public class XMLSpider implements FredPlugin, FredPluginHTTP, FredPluginThreadless,  FredPluginHTTPAdvanced,HttpPlugin, ClientCallback, USKCallback{

	long tProducedIndex;
	private TreeMap tMap = new TreeMap();
	int count;
	// URIs visited, or fetching, or queued. Added once then forgotten about.
	private final HashSet visitedURIs = new HashSet();
	private final HashSet urisWithWords = new HashSet();
	private final HashSet idsWithWords = new HashSet();
	private final HashSet failedURIs = new HashSet();
	private final HashSet queuedURISet = new HashSet();
	private final LinkedList queuedURIList = new LinkedList();
	private final HashMap runningFetchesByURI = new HashMap();
	private final HashMap urisByWord = new HashMap();
	private final HashMap idsByWord = new HashMap();
	private final HashMap titlesOfURIs = new HashMap();
	private final HashMap titlesOfIds = new HashMap();
	private final HashMap uriIds = new HashMap();
	private final HashMap idUris = new HashMap();
	private final HashMap outlinks = new HashMap();
	private final HashMap inlinks = new HashMap();
	private Vector indices;
	private int match;
	private int id;
	private Vector list;
	private boolean indexing ;
	
	private static final int minTimeBetweenEachIndexRewriting = 10;
/**
 * DEFAULT_INDEX_DIR is the directory where the generated indices are stored.
 * Needs to be created before it can be used
 */
	private static final String DEFAULT_INDEX_DIR = "myindex4/";
	public Set allowedMIMETypes;
	private static final int MAX_ENTRIES = 10;
	private static final String pluginName = "XML spider";
	/**
	 * This gives the allowed fraction of total time spent on generating indices
	 * max value = 1; min value > 0 
	 */
	private static final double MAX_TIME_SPENT_INDEXING = 0.5;
	
	private static final String indexTitle= "XMLSpider index";
	private static final String indexOwner = "Freenet";
	private static final String indexOwnerEmail = null;
	private final HashMap sizeOfURIs = new HashMap(); /* String (URI) -> Long */
	private final HashMap mimeOfURIs = new HashMap(); /* String (URI) -> String */
//	private final HashMap lastPositionByURI = new HashMap(); /* String (URI) -> Integer */ /* Use to determine word position on each uri */
	private final HashMap lastPositionById = new HashMap();
//	private final HashMap positionsByWordByURI = new HashMap(); /* String (URI) -> HashMap (String (word) -> Integer[] (Positions)) */
	private final HashMap positionsByWordById = new HashMap();
	// Can have many; this limit only exists to save memory.
	private static final int maxParallelRequests = 100;
	private int maxShownURIs = 15;
	private HashMap urisToNumbers;
	private NodeClientCore core;
	private FetchContext ctx;
	private final short PRIORITY_CLASS = RequestStarter.BULK_SPLITFILE_PRIORITY_CLASS;
	private boolean stopped = true;
	PluginRespirator pr;
	

	private synchronized void queueURI(FreenetURI uri) {
		//not adding the html condition
		if((uri.getKeyType()).equals("USK")){
			if(uri.getSuggestedEdition() < 0)
				uri = uri.setSuggestedEdition((-1)* uri.getSuggestedEdition());
			try{
			uri = ((USK.create(uri)).getSSK()).getURI();
			//all uris are added as ssk
			(ctx.uskManager).subscribe(USK.create(uri),this, false, this);	
			}
			catch(Exception e){}
		}
		
		if ((!visitedURIs.contains(uri)) && queuedURISet.add(uri)) {
			queuedURIList.addLast(uri);
			visitedURIs.add(uri);
			uriIds.put(uri, id);
			idUris.put(id, uri);
			id++;
			
			//the page object of the client will contain the uri of the current page
		}
	}

	private void startSomeRequests() {

		
		FreenetURI[] initialURIs = core.bookmarkManager.getBookmarkURIs();
		for (int i = 0; i < initialURIs.length; i++)
		{
		queueURI(initialURIs[i]);
		}
					
		ArrayList toStart = null;
		synchronized (this) {
			if (stopped) {
				return;
			}
			int running = runningFetchesByURI.size();
			int queued = queuedURIList.size();
			
			if ((running >= maxParallelRequests) || (queued == 0))
				return;
			
			toStart = new ArrayList(Math.min(maxParallelRequests - running, queued));
			
			for (int i = running; i < maxParallelRequests; i++) {
				if (queuedURIList.isEmpty())
					break;
				FreenetURI uri = (FreenetURI) queuedURIList.removeFirst();
				queuedURISet.remove(uri);
//				if((uri.getKeyType()).equals("USK")){
//				if(uri.getSuggestedEdition() < 0)
//					uri = uri.setSuggestedEdition((-1)* uri.getSuggestedEdition());
//				try{
//					(ctx.uskManager).subscribe(USK.create(uri),this, false, this);	
//				}catch(Exception e){
//					
//				}
				
	//			}
				ClientGetter getter = makeGetter(uri);
				toStart.add(getter);
				}
		}
			for (int i = 0; i < toStart.size(); i++) {
				
			ClientGetter g = (ClientGetter) toStart.get(i);
			try {
				runningFetchesByURI.put(g.getURI(), g);
				g.start();
				FileWriter outp = new FileWriter("logfile2",true);
				outp.write("URI "+g.getURI().toString()+'\n');
				
				outp.close();
				} catch (FetchException e) {
					onFailure(e, g);
				}
				catch (IOException e){
					Logger.error(this, "the logfile can not be written"+e.toString(), e);
				}
		
			}
		//}
				
	}
	

	private ClientGetter makeGetter(FreenetURI uri) {
		ClientGetter g = new ClientGetter(this, core.requestStarters.chkFetchScheduler, core.requestStarters.sskFetchScheduler, uri, ctx, PRIORITY_CLASS, this, null, null);
		return g;
	}

	public void onSuccess(FetchResult result, ClientGetter state) {
		FreenetURI uri = state.getURI();

		synchronized (this) {
			runningFetchesByURI.remove(uri);
		}
		startSomeRequests();
		ClientMetadata cm = result.getMetadata();
		Bucket data = result.asBucket();
		String mimeType = cm.getMIMEType();
		
		sizeOfURIs.put(uri.toString(), new Long(data.size()));
		mimeOfURIs.put(uri.toString(), mimeType);
		PageCallBack page = new PageCallBack();
		page.id = (Integer) uriIds.get(uri);
		inlinks.put(page.id, new Vector());
		outlinks.put(page.id, new Vector());
		
		try{
	    FileWriter output = new FileWriter("logfile",true);
	    output.write(uri.toString()+" page " + page.id +"\n");
	    output.close();
		}
		catch(Exception e){
			Logger.error(this, "The uri could not be removed from running "+e.toString(), e);
		}
		try {
			ContentFilter.filter(data, ctx.bucketFactory, mimeType, uri.toURI("http://127.0.0.1:8888/"), page);
		} catch (UnsafeContentTypeException e) {
			return; // Ignore
		} catch (IOException e) {
			Logger.error(this, "Bucket error?: " + e, e);
		} catch (URISyntaxException e) {
			Logger.error(this, "Internal error: " + e, e);
		} finally {
			data.free();
		}
	}
	
	public void onFailure(FetchException e, ClientGetter state) {
		FreenetURI uri = state.getURI();
//		try{
//			FileWriter outp = new FileWriter("failed",true);
//			outp.write("failed "+e.toString()+" for "+uri+'\n');
//			outp.close();
//			
//		}catch(Exception e2){
//			
//		}
		synchronized (this) {
			runningFetchesByURI.remove(uri);
			failedURIs.add(uri);
		}
		if (e.newURI != null)
			queueURI(e.newURI);
//		else
//			queueURI(uri);
		startSomeRequests();
		
		
	}

	public void onSuccess(BaseClientPutter state) {
		// Ignore
	}

	public void onFailure(InsertException e, BaseClientPutter state) {
		// Ignore
	}

	public void onGeneratedURI(FreenetURI uri, BaseClientPutter state) {
		// Ignore
	}

	/**
 * generates the main index file that can be used by librarian for searching in the list of
 * subindices
 *  
 * @param void
 * @author swati 
 * @throws IOException
 * @throws NoSuchAlgorithmException
 */
	private synchronized void produceIndex2() throws IOException,NoSuchAlgorithmException {
		// Produce the main index file.
		
		//the number of bits to consider for matching 
		
	
//		if (urisByWord.isEmpty() || urisWithWords.isEmpty()) {
//			System.out.println("No URIs with words");
//			return;
//		}
		
		if (idsByWord.isEmpty() || idsWithWords.isEmpty()) {
			System.out.println("No URIs with words");
			return;
		}
		File outputFile = new File(DEFAULT_INDEX_DIR+"index.xml");
		StreamResult resultStream;
		resultStream = new StreamResult(outputFile);

		/* Initialize xml builder */
		Document xmlDoc = null;
		DocumentBuilderFactory xmlFactory = null;
		DocumentBuilder xmlBuilder = null;
		DOMImplementation impl = null;
		Element rootElement = null;

		xmlFactory = DocumentBuilderFactory.newInstance();


		try {
			xmlBuilder = xmlFactory.newDocumentBuilder();
		} catch(javax.xml.parsers.ParserConfigurationException e) {
			/* Will (should ?) never happen */
			Logger.error(this, "Spider: Error while initializing XML generator: "+e.toString());
			return;
		}

		impl = xmlBuilder.getDOMImplementation();
		/* Starting to generate index */
		xmlDoc = impl.createDocument(null, "main_index", null);
		rootElement = xmlDoc.getDocumentElement();

		/* Adding header to the index */
		Element headerElement = xmlDoc.createElement("header");

		/* -> title */
		Element subHeaderElement = xmlDoc.createElement("title");
		Text subHeaderText = xmlDoc.createTextNode(indexTitle);
		
		subHeaderElement.appendChild(subHeaderText);
		headerElement.appendChild(subHeaderElement);

		/* -> owner */
		subHeaderElement = xmlDoc.createElement("owner");
		subHeaderText = xmlDoc.createTextNode(indexOwner);
		
		subHeaderElement.appendChild(subHeaderText);
		headerElement.appendChild(subHeaderElement);
		
		/* -> owner email */
		if(indexOwnerEmail != null) {
			subHeaderElement = xmlDoc.createElement("email");
			subHeaderText = xmlDoc.createTextNode(indexOwnerEmail);
			
			subHeaderElement.appendChild(subHeaderText);
			headerElement.appendChild(subHeaderElement);
		}

		
		//String[] words = (String[]) urisByWord.keySet().toArray(new String[urisByWord.size()]);
		//Arrays.sort(words);
		
		Element prefixElement = xmlDoc.createElement("prefix");
		//prefixElement.setAttribute("value",match+"");
		//this match will be set after processing the TreeMap
	

		
		//all index files are ready
		/* Adding word index */
		Element keywordsElement = xmlDoc.createElement("keywords");
		for(int i = 0;i<indices.size();i++){
			//generateSubIndex(DEFAULT_INDEX_DIR+"index_"+Integer.toHexString(i)+".xml");
			Element subIndexElement = xmlDoc.createElement("subIndex");
//			if(i<=9)
//			subIndexElement.setAttribute("key",i+"");
//			else
//				subIndexElement.setAttribute("key",Integer.toHexString(i));
			subIndexElement.setAttribute("key", (String) indices.elementAt(i));
			//the subindex element key will contain the bits used for matching in that subindex
			keywordsElement.appendChild(subIndexElement);
		}
		
		prefixElement.setAttribute("value",match+"");
		// make sure that prefix is the first child of root Element
		rootElement.appendChild(prefixElement);
		rootElement.appendChild(headerElement);
		
		//rootElement.appendChild(filesElement);
		rootElement.appendChild(keywordsElement);

		/* Serialization */
		DOMSource domSource = new DOMSource(xmlDoc);
		TransformerFactory transformFactory = TransformerFactory.newInstance();
		Transformer serializer;

		try {
			serializer = transformFactory.newTransformer();
		} catch(javax.xml.transform.TransformerConfigurationException e) {
			Logger.error(this, "Spider: Error while serializing XML (transformFactory.newTransformer()): "+e.toString());
			return;
		}

		serializer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		serializer.setOutputProperty(OutputKeys.INDENT,"yes");
		
		/* final step */
		try {
			serializer.transform(domSource, resultStream);
		} catch(javax.xml.transform.TransformerException e) {
			Logger.error(this, "Spider: Error while serializing XML (transform()): "+e.toString());
			return;
		}

		if(Logger.shouldLog(Logger.MINOR, this))
			Logger.minor(this, "Spider: indexes regenerated.");
	
	//the main xml file is generated 
	//now as each word is generated enter it into the respective subindex
	//now the parsing will start and nodes will be added as needed 
		

	}
	/**
	 * Generates the subindices. 
	 * Each index has less than {@code MAX_ENTRIES} words.
	 * The original treemap is split into several sublists indexed by the common substring
	 * of the hash code of the words
	 * @throws Exception
	 */
	private synchronized void generateIndex2() throws Exception{
		// now we the tree map and we need to use the sorted (md5s) to generate the xml indices
	
		
		if (idsByWord.isEmpty() || idsWithWords.isEmpty()) {
			System.out.println("No URIs with words");
			return;
		}
		
	//	FreenetURI[] uris = (FreenetURI[]) urisWithWords.toArray(new FreenetURI[urisWithWords.size()]);
		Integer[] ids = (Integer[]) idsWithWords.toArray(new Integer[idsWithWords.size()]);
//		urisToNumbers = new HashMap();
//		for (int i = 0; i < uris.length; i++) {
//			urisToNumbers.put(uris[i], new Integer(i));
//			}
		indices = new Vector();
		int prefix = 1;
		match = 1;
		Vector list = new Vector();
		//String str = tMap.firstKey();
		Iterator it = tMap.keySet().iterator();
		 FileWriter outp = new FileWriter("indexing");
		outp.write("size = "+tMap.size()+"\n");
		outp.close();
		String str = (String) it.next();
		int i = 0,index =0;
		while(it.hasNext())
		{
		 outp = new FileWriter("indexing",true);
			String key =(String) it.next();
			outp.write(key + "\n");
			outp.close();
			if(key.substring(0, prefix).equals(str.substring(0, prefix))) 
				{i++;
				list.add(key);
				}
			else {
		generateSubIndex(prefix,list);
		str = key;
		list = new Vector();
		}
		}
		
		generateSubIndex(prefix,list);
	}
	private synchronized Vector subVector(Vector list, int begin, int end){
		Vector tmp = new Vector();
		for(int i = begin;i<end+1;i++) tmp.add(list.elementAt(i));
		return tmp;
	}
	
	private synchronized void generateSubIndex(int p,Vector list) throws Exception{
		
		if(list.size() < MAX_ENTRIES)
		{
			//the index can be generated from this list
			generateXML(list,p);
		}
		else
		{
			//this means that prefix needs to be incremented
			if(match <= p) match = p+1; 
			int prefix = p+1;
			int i =0;
			String str = (String) list.elementAt(i);
			int index=0;
			while(i<list.size())
			{
				String key = (String) list.elementAt(i);
				if((key.substring(0, prefix)).equals(str.substring(0, prefix))) 
					{
					//index = i;
					i++;
					}
				else {
					//generateXML(subVector(list,index,i-1),prefix);
					generateSubIndex(prefix,subVector(list,index,i-1));
					index = i;
					str = key;
				}
				

			}
			generateSubIndex(prefix,subVector(list,index,i-1));
		}
	}	
		

	private synchronized void generateXML (Vector list, int prefix) throws Exception
	{
		FileWriter outp = new FileWriter("gen",true);
		
		
		String p = ((String) list.elementAt(0)).substring(0, prefix);
		outp.write("inside gen xml + "+p+"\n");
		
		indices.add(p);
		File outputFile = new File(DEFAULT_INDEX_DIR+"index_"+p+".xml");
		//indices.add(p);
		StreamResult resultStream;
		resultStream = new StreamResult(outputFile);

		/* Initialize xml builder */
		Document xmlDoc = null;
		DocumentBuilderFactory xmlFactory = null;
		DocumentBuilder xmlBuilder = null;
		DOMImplementation impl = null;
		Element rootElement = null;

		xmlFactory = DocumentBuilderFactory.newInstance();


		try {
			xmlBuilder = xmlFactory.newDocumentBuilder();
		} catch(javax.xml.parsers.ParserConfigurationException e) {
			/* Will (should ?) never happen */
			Logger.error(this, "Spider: Error while initializing XML generator: "+e.toString());
			return;
		}


		impl = xmlBuilder.getDOMImplementation();

		/* Starting to generate index */

		xmlDoc = impl.createDocument(null, "sub_index", null);
		rootElement = xmlDoc.getDocumentElement();

		/* Adding header to the index */
		Element headerElement = xmlDoc.createElement("header");

		/* -> title */
		Element subHeaderElement = xmlDoc.createElement("title");
		Text subHeaderText = xmlDoc.createTextNode(indexTitle);
		
		subHeaderElement.appendChild(subHeaderText);
		headerElement.appendChild(subHeaderElement);

			
		
		Element filesElement = xmlDoc.createElement("files"); /* filesElement != fileElement */

		Element EntriesElement = xmlDoc.createElement("entries");
		
		EntriesElement.setNodeValue(list.size()+"");
		outp.write("size = "+list.size()+"\n");
		EntriesElement.setAttribute("value", list.size()+"");
		//all index files are ready
		/* Adding word index */
		Element keywordsElement = xmlDoc.createElement("keywords");
		//words to be added 
		Vector fileid = new Vector();
		for(int i =0;i<list.size();i++)
		{
			Element wordElement = xmlDoc.createElement("word");
			String str = (String) tMap.get(list.elementAt(i));
			outp.write("word "+str+"\n");
			wordElement.setAttribute("v",str );
			//FreenetURI[] urisForWord = (FreenetURI[]) urisByWord.get(str);
			Integer[] idsForWord = (Integer[]) idsByWord.get(str);
//			
			for (int j = 0; j < idsForWord.length; j++) {
				Integer id = idsForWord[j];
				//Integer x = (Integer) urisToNumbers.get(uri);
				Integer x = id;
				outp.write("x "+x+"\n");
				if (x == null) {
					Logger.error(this, "Eh?");
					continue;
				}
//
				Element uriElement = xmlDoc.createElement("file");
				Element fileElement = xmlDoc.createElement("file");
				uriElement.setAttribute("id", x.toString());
				fileElement.setAttribute("id", x.toString());
				//fileElement.setAttribute("key", uri.toString());
				outp.write("uri "+(idUris.get(id)).toString()+"\n");
				fileElement.setAttribute("key",(idUris.get(id)).toString());
////				/* Position by position */
				//HashMap positionsForGivenWord = (HashMap)positionsByWordByURI.get(uri.toString());
				HashMap positionsForGivenWord = (HashMap)positionsByWordById.get(x);
				Integer[] positions = (Integer[])positionsForGivenWord.get(str);

				StringBuffer positionList = new StringBuffer();

				for(int k=0; k < positions.length ; k++) {
					if(k!=0)
						positionList.append(',');

					positionList.append(positions[k].toString());
				}
				
				uriElement.appendChild(xmlDoc.createTextNode(positionList.toString()));
			
				wordElement.appendChild(uriElement);
				if(!fileid.contains(x))
				{
					fileid.add(x);
					filesElement.appendChild(fileElement);
				}
			}
			
			//Element keywordsElement = (Element) root.getElementsByTagName("keywords").item(0);
			keywordsElement.appendChild(wordElement);
				
		}
		
		rootElement.appendChild(EntriesElement);
		rootElement.appendChild(headerElement);
		rootElement.appendChild(filesElement);
		rootElement.appendChild(keywordsElement);

		/* Serialization */
		DOMSource domSource = new DOMSource(xmlDoc);
		TransformerFactory transformFactory = TransformerFactory.newInstance();
		Transformer serializer;

		try {
			serializer = transformFactory.newTransformer();
		} catch(javax.xml.transform.TransformerConfigurationException e) {
			Logger.error(this, "Spider: Error while serializing XML (transformFactory.newTransformer()): "+e.toString());
			return;
		}


		serializer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		serializer.setOutputProperty(OutputKeys.INDENT,"yes");
		
		/* final step */
		try {
			serializer.transform(domSource, resultStream);
		} catch(javax.xml.transform.TransformerException e) {
			Logger.error(this, "Spider: Error while serializing XML (transform()): "+e.toString());
			return;
		}

		if(Logger.shouldLog(Logger.MINOR, this))
			Logger.minor(this, "Spider: indexes regenerated.");
		outp.close();
	}

		
	public String search(String str,NodeList list) throws Exception
	{
		int prefix = str.length();
		for(int i = 0;i<list.getLength();i++){
			Element subIndex = (Element) list.item(i);
			String key = subIndex.getAttribute("key");
			if(key.equals(str)) return key;
		}
		return search(str.substring(0, prefix-1),list);
	}

	
	public void handleGet(HTTPRequest request, ToadletContext context) throws IOException, ToadletContextClosedException {
		String action = request.getParam("action");
		PageMaker pageMaker = context.getPageMaker();
		if ((action == null) || (action.length() == 0)) {
			MultiValueTable responseHeaders = new MultiValueTable();
			responseHeaders.put("Location", "?action=list");
			context.sendReplyHeaders(301, "Redirect", responseHeaders, "text/html; charset=utf-8", 0);
			return;
		} else if ("list".equals(action)) {
			
			String listName = request.getParam("listName", null);
			HTMLNode pageNode = pageMaker.getPageNode("The XML Spider", context);
			HTMLNode contentNode = pageMaker.getContentNode(pageNode);
			/* create copies for multi-threaded use */
			if (listName == null) {
				Map runningFetches = new HashMap(runningFetchesByURI);
				List queued = new ArrayList(queuedURIList);
				Set visited = new HashSet(visitedURIs);
				Set failed = new HashSet(failedURIs);
				contentNode.addChild(createNavbar(pageMaker, runningFetches.size(), queued.size(), visited.size(), failed.size()));
				contentNode.addChild(createAddBox(pageMaker, context));
				contentNode.addChild(createList(pageMaker, "Running FetcheIIIs", "running", runningFetches.keySet(), maxShownURIs));
				contentNode.addChild(createList(pageMaker, "Queued URIs", "queued", queued, maxShownURIs));
				contentNode.addChild(createList(pageMaker, "Visited URIs", "visited", visited, maxShownURIs));
				contentNode.addChild(createList(pageMaker, "Failed URIs", "failed", failed, maxShownURIs));
			} else {
				contentNode.addChild(createBackBox(pageMaker));
				if ("failed".equals(listName)) {
					Set failed = new HashSet(failedURIs);
					contentNode.addChild(createList(pageMaker, "Failed URIs", "failed", failed, -1));	
				} else if ("visited".equals(listName)) {
					Set visited = new HashSet(visitedURIs);
					contentNode.addChild(createList(pageMaker, "Visited URIs", "visited", visited, -1));
				} else if ("queued".equals(listName)) {
					List queued = new ArrayList(queuedURIList);
					contentNode.addChild(createList(pageMaker, "Queued URIs", "queued", queued, -1));
				} else if ("running".equals(listName)) {
					Map runningFetches = new HashMap(runningFetchesByURI);
					contentNode.addChild(createList(pageMaker, "Running Fetches", "running", runningFetches.keySet(), -1));
				}
			}
			MultiValueTable responseHeaders = new MultiValueTable();
			byte[] responseBytes = pageNode.generate().getBytes("utf-8");
			context.sendReplyHeaders(200, "OK", responseHeaders, "text/html; charset=utf-8", responseBytes.length);
			context.writeData(responseBytes);
		} else if ("add".equals(action)) {
			String uriParam = request.getParam("key");
			try {
				FreenetURI uri = new FreenetURI(uriParam);
				synchronized (this) {
					failedURIs.remove(uri);
					visitedURIs.remove(uri);
				}
				queueURI(uri);
				startSomeRequests();
			} catch (MalformedURLException mue1) {
				sendSimpleResponse(context, "URL invalid", "The given URI is not valid.");
				return;
			}
			MultiValueTable responseHeaders = new MultiValueTable();
			responseHeaders.put("Location", "?action=list");
			context.sendReplyHeaders(301, "Redirect", responseHeaders, "text/html; charset=utf-8", 0);
			return;
		}
	}

	public void handlePost(HTTPRequest request, ToadletContext context) throws IOException {
	}
	
	private void sendSimpleResponse(ToadletContext context, String title, String message) throws ToadletContextClosedException, IOException {
		PageMaker pageMaker = context.getPageMaker();
		HTMLNode pageNode = pageMaker.getPageNode(title, context);
		HTMLNode contentNode = pageMaker.getContentNode(pageNode);
		HTMLNode infobox = contentNode.addChild(pageMaker.getInfobox("infobox-alter", title));
		HTMLNode infoboxContent = pageMaker.getContentNode(infobox);
		infoboxContent.addChild("#", message);
		byte[] responseBytes = pageNode.generate().getBytes("utf-8");
		context.sendReplyHeaders(200, "OK", new MultiValueTable(), "text/html; charset=utf-8", responseBytes.length);
		context.writeData(responseBytes);
	}
	
	private HTMLNode createBackBox(PageMaker pageMaker) {
		HTMLNode backbox = pageMaker.getInfobox((String) null);
		HTMLNode backContent = pageMaker.getContentNode(backbox);
		backContent.addChild("#", "Return to the ");
		backContent.addChild("a", "href", "?action=list", "list of all URIs");
		backContent.addChild("#", ".");
		return backbox;
	}
	
	private HTMLNode createAddBox(PageMaker pageMaker, ToadletContext ctx) {
		HTMLNode addBox = pageMaker.getInfobox("Add a URI");
		HTMLNode formNode = pageMaker.getContentNode(addBox).addChild("form", new String[] { "action", "method" }, new String[] { "", "get" });
		formNode.addChild("input", new String[] { "type", "name", "value" }, new String[] { "hidden", "action", "add" });
		formNode.addChild("input", new String[] { "type", "size", "name", "value" }, new String[] { "text", "40", "key", "" });
		formNode.addChild("input", new String[] { "type", "value" }, new String[] { "submit", "Add URI" });
		return addBox;
	}

	private HTMLNode createNavbar(PageMaker pageMaker, int running, int queued, int visited, int failed) {
		HTMLNode navbar = pageMaker.getInfobox("navbar", "Page Navigation");
		HTMLNode list = pageMaker.getContentNode(navbar).addChild("ul");
		list.addChild("li").addChild("a", "href", "#running", "Running (" + running + ')');
		list.addChild("li").addChild("a", "href", "#queued", "Queued (" + queued + ')');
		list.addChild("li").addChild("a", "href", "#visited", "Visited (" + visited + ')');
		list.addChild("li").addChild("a", "href", "#failed", "Failed (" + failed + ')');
		return navbar;
	}

	private HTMLNode createList(PageMaker pageMaker, String listName, String anchorName, Collection collection, int maxCount) {
		HTMLNode listNode = new HTMLNode("div");
		listNode.addChild("a", "name", anchorName);
		HTMLNode listBox = pageMaker.getInfobox(listName);
		HTMLNode listContent = pageMaker.getContentNode(listBox);
		listNode.addChild(listBox);
		Iterator collectionItems = collection.iterator();
		int itemCount = 0;
		while (collectionItems.hasNext()) {
			FreenetURI uri = (FreenetURI) collectionItems.next();
			listContent.addChild("#", uri.toString());
			listContent.addChild("br");
			if (itemCount++ == maxCount) {
				listContent.addChild("br");
				listContent.addChild("a", "href", "?action=list&listName=" + anchorName, "Show all\u2026");
				break;
			}
		}
		return listNode;
	}

	/**
	 * @see freenet.oldplugins.plugin.Plugin#getPluginName()
	 */
	public String getPluginName() {
		return pluginName;
	}

	/**
	 * @see freenet.oldplugins.plugin.Plugin#setPluginManager(freenet.oldplugins.plugin.PluginManager)
	 */
	public void setPluginManager(PluginManager pluginManager) {
		
		this.core = pluginManager.getClientCore();
		this.ctx = core.makeClient((short) 0).getFetchContext();
		ctx.maxSplitfileBlockRetries = 10;
		ctx.maxNonSplitfileRetries = 10;
		ctx.maxTempLength = 2 * 1024 * 1024;
		ctx.maxOutputLength = 2 * 1024 * 1024;
		allowedMIMETypes = new HashSet();
		allowedMIMETypes.add(new String("text/html"));
		allowedMIMETypes.add(new String("text/plain"));
		allowedMIMETypes.add(new String("application/xhtml+xml"));
	//	allowedMIMETypes.add(new String("application/zip"));
		ctx.allowedMIMETypes = new HashSet(allowedMIMETypes);
	//	ctx.allowedMIMETypes.add("text/html"); 
		tProducedIndex = System.currentTimeMillis();
		indexing = true;
	}


	/**
	 * @see freenet.oldplugins.plugin.Plugin#startPlugin()
	 */
	public void startPlugin() {
		stopped = false;
	}

	/**
	 * @see freenet.oldplugins.plugin.Plugin#stopPlugin()
	 */
	public void stopPlugin() {
		synchronized (this) {
			stopped = true;
			queuedURIList.clear();
		}
	}

	public void onMajorProgress() {
		// Ignore
	}

	public void onFetchable(BaseClientPutter state) {
		// Ignore
	}
	private static String convertToHex(byte[] data) {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < data.length; i++) {
        	int halfbyte = (data[i] >>> 4) & 0x0F;
        	int two_halfs = 0;
        	do {
	        	if ((0 <= halfbyte) && (halfbyte <= 9))
	                buf.append((char) ('0' + halfbyte));
	            else
	            	buf.append((char) ('a' + (halfbyte - 10)));
	        	halfbyte = data[i] & 0x0F;
        	} while(two_halfs++ < 1);
        }
        return buf.toString();
    }
	//this function will return the String representation of the MD5 hash for the input string 
	public static String MD5(String text) throws NoSuchAlgorithmException, UnsupportedEncodingException  {
		MessageDigest md;
		md = MessageDigest.getInstance("MD5");
		byte[] md5hash = new byte[32];
		md.update(text.getBytes("iso-8859-1"), 0, text.length());
		md5hash = md.digest();
		return convertToHex(md5hash);
	}
	
	public void generateSubIndex(String filename){
//generates the new subIndex
		File outputFile = new File(filename);
		StreamResult resultStream;
		resultStream = new StreamResult(outputFile);

		/* Initialize xml builder */
		Document xmlDoc = null;
		DocumentBuilderFactory xmlFactory = null;
		DocumentBuilder xmlBuilder = null;
		DOMImplementation impl = null;
		Element rootElement = null;

		xmlFactory = DocumentBuilderFactory.newInstance();


		try {
			xmlBuilder = xmlFactory.newDocumentBuilder();
		} catch(javax.xml.parsers.ParserConfigurationException e) {
			/* Will (should ?) never happen */
			Logger.error(this, "Spider: Error while initializing XML generator: "+e.toString());
			return;
		}


		impl = xmlBuilder.getDOMImplementation();

		/* Starting to generate index */

		xmlDoc = impl.createDocument(null, "sub_index", null);
		rootElement = xmlDoc.getDocumentElement();

		/* Adding header to the index */
		Element headerElement = xmlDoc.createElement("header");

		/* -> title */
		Element subHeaderElement = xmlDoc.createElement("title");
		Text subHeaderText = xmlDoc.createTextNode(indexTitle);
		
		subHeaderElement.appendChild(subHeaderText);
		headerElement.appendChild(subHeaderElement);

		/* -> owner */
		subHeaderElement = xmlDoc.createElement("owner");
		subHeaderText = xmlDoc.createTextNode(indexOwner);
		
		subHeaderElement.appendChild(subHeaderText);
		headerElement.appendChild(subHeaderElement);
		
	
		/* -> owner email */
		if(indexOwnerEmail != null) {
			subHeaderElement = xmlDoc.createElement("email");
			subHeaderText = xmlDoc.createTextNode(indexOwnerEmail);
			
			subHeaderElement.appendChild(subHeaderText);
			headerElement.appendChild(subHeaderElement);
		}

		
		Element filesElement = xmlDoc.createElement("files"); /* filesElement != fileElement */

		Element EntriesElement = xmlDoc.createElement("entries");
		EntriesElement.setNodeValue("0");
		EntriesElement.setAttribute("value", "0");
		//all index files are ready
		/* Adding word index */
		Element keywordsElement = xmlDoc.createElement("keywords");
		
		rootElement.appendChild(EntriesElement);
		rootElement.appendChild(headerElement);
		rootElement.appendChild(filesElement);
		rootElement.appendChild(keywordsElement);

		/* Serialization */
		DOMSource domSource = new DOMSource(xmlDoc);
		TransformerFactory transformFactory = TransformerFactory.newInstance();
		Transformer serializer;

		try {
			serializer = transformFactory.newTransformer();
		} catch(javax.xml.transform.TransformerConfigurationException e) {
			Logger.error(this, "Spider: Error while serializing XML (transformFactory.newTransformer()): "+e.toString());
			return;
		}


		serializer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		serializer.setOutputProperty(OutputKeys.INDENT,"yes");
		
		/* final step */
		try {
			serializer.transform(domSource, resultStream);
		} catch(javax.xml.transform.TransformerException e) {
			Logger.error(this, "Spider: Error while serializing XML (transform()): "+e.toString());
			return;
		}

		if(Logger.shouldLog(Logger.MINOR, this))
			Logger.minor(this, "Spider: indexes regenerated.");
	}
	
public void terminate(){
	synchronized (this) {
		stopped = true;
		queuedURIList.clear();
	}
}
	
public void runPlugin(PluginRespirator pr){
	this.pr = pr;
	this.id = 0;
	this.core = pr.getNode().clientCore;
	this.ctx = core.makeClient((short) 0).getFetchContext();
	ctx.maxSplitfileBlockRetries = 10;
	ctx.maxNonSplitfileRetries = 10;
	ctx.maxTempLength = 2 * 1024 * 1024;
	ctx.maxOutputLength = 2 * 1024 * 1024;
	allowedMIMETypes = new HashSet();
	allowedMIMETypes.add(new String("text/html"));
	allowedMIMETypes.add(new String("text/plain"));
	allowedMIMETypes.add(new String("application/xhtml+xml"));
//	allowedMIMETypes.add(new String("application/zip"));
	ctx.allowedMIMETypes = new HashSet(allowedMIMETypes);
//	ctx.allowedMIMETypes.add("text/html"); 
	tProducedIndex = System.currentTimeMillis();
	indexing = true;
	stopped = false;
	count = 0;
	
	//startPlugin();
	Thread starterThread = new Thread("Spider Plugin Starter") {
		public void run() {
			try{
				Thread.sleep(30 * 1000); // Let the node start up
			} catch (InterruptedException e){}
			startSomeRequests();
		}
	};
	starterThread.setDaemon(true);
	starterThread.start();
}

public String handleHTTPGet(HTTPRequest request) throws PluginHTTPException{
	StringBuffer out = new StringBuffer();
	// need to produce pretty html
	//later fredpluginhttpadvanced will give the interface
	//this brings us to the page from visit
	String listname = request.getParam("list");
	if(listname.length() != 0)
	{
		appendDefaultHeader(out,null);
		out.append("<p><h4>"+listname+" URIs</h4></p>");
		appendList(listname,out,null);
		return out.toString();
	}
	appendDefaultPageStart(out,null);
	String uriParam = request.getParam("adduri");
	if(uriParam != null && uriParam.length() != 0)
		{
		try {
			FreenetURI uri = new FreenetURI(uriParam);
			synchronized (this) {
				failedURIs.remove(uri);
				visitedURIs.remove(uri);
			}
			out.append("<p>URI added :"+uriParam+"</p>");
			queueURI(uri);
			startSomeRequests();
		} catch (MalformedURLException mue1) {
			out.append("<p>MalFormed URI: "+uriParam+"</p");
		}
		}
	return out.toString();
}
private void appendList(String listname, StringBuffer out, String stylesheet)
{
	Iterator it = (runningFetchesByURI.keySet()).iterator();
	if(listname.equals("running"))
		it = (runningFetchesByURI.keySet()).iterator();
	if(listname.equals("visited"))
		it = (new HashSet(visitedURIs)).iterator();
	if(listname.equals("queued"))
		it = (new ArrayList(queuedURIList)).iterator();
	if(listname.equals("failed"))
		it = (new HashSet(failedURIs)).iterator();
	while(it.hasNext())
		out.append("<code>"+it.next().toString()+"</code><br/>");
}
private void appendDefaultPageStart(StringBuffer out, String stylesheet) {
	
	out.append("<HTML><HEAD><TITLE>" + pluginName + "</TITLE>");
	if(stylesheet != null)
		out.append("<link href=\""+stylesheet+"\" type=\"text/css\" rel=\"stylesheet\" />");
	out.append("</HEAD><BODY>\n");
	out.append("<CENTER><H1>" + pluginName + "</H1><BR/><BR/><BR/>\n");
	out.append("Add uri:");
	out.append("<form method=\"GET\"><input type=\"text\" name=\"adduri\" /><br/><br/>");
	out.append("<input type=\"submit\" value=\"Add uri\" /></form>");
	Set runningFetches = runningFetchesByURI.keySet();
	out.append("<p><h3>Running Fetches</h3></p>");
	Set visited = new HashSet(visitedURIs);
	List queued = new ArrayList(queuedURIList);
	
	Set failed = new HashSet(failedURIs);
	Iterator it=queued.iterator();
	out.append("<br/>Size :"+runningFetches.size());
	appendList(runningFetches,out,stylesheet);
	out.append("<p><a href=\"?list="+"running"+"\">Show all</a><br/></p>");
	out.append("<br/>Size :"+queued.size());
	int i = 0;
	while(it.hasNext()){
		if(i<=maxShownURIs){
		out.append("<code>"+it.next().toString()+"</code><br/>");
		}
		else break;
		i++;
	}
	out.append("<p><a href=\"?list="+"queued"+"\">Show all</a><br/></p>");
	out.append("<br/>Size :"+visited.size());
	appendList(visited,out,stylesheet);
	out.append("<p><a href=\"?list="+"visited"+"\">Show all</a><br/></p>");
	out.append("<br/>Size :"+failed.size());
	appendList(failed,out,stylesheet);
	out.append("<p><a href=\"?list="+"failed"+"\">Show all</a><br/></p>");
	
	
}
private void appendDefaultHeader(StringBuffer out, String stylesheet){
	out.append("<HTML><HEAD><TITLE>" + pluginName + "</TITLE>");
	if(stylesheet != null)
		out.append("<link href=\""+stylesheet+"\" type=\"text/css\" rel=\"stylesheet\" />");
	out.append("</HEAD><BODY>\n");
	out.append("<CENTER><H1>" + pluginName + "</H1><BR/><BR/><BR/>\n");
	out.append("Add uri:");
	out.append("<form method=\"GET\"><input type=\"text\" name=\"adduri\" /><br/><br/>");
	out.append("<input type=\"submit\" value=\"Add uri\" /></form>");
}
private void appendList(Set  list,StringBuffer out, String stylesheet){
	Iterator it = list.iterator();
	int i = 0;
	while(it.hasNext()){
		if(i<=maxShownURIs){
		out.append("<code>"+it.next().toString()+"</code><br/>");
		}
		else{
			//out.append("<form method=\"GET\"><input type=\"submit\" name=\"Showall\" />");
//			if(listname.equals("visited"))
//			out.append("<p><a href=\"?list="+listname+">Showall visited</a><br/></p>");
//			if(listname.equals("failed"))
//				out.append("<p><a href=\"?list="+listname+">Showall failed</a><br/></p>");
			break;
		}
		i++;
		
	}
	
}

public class PageCallBack implements FoundURICallback{
	int id;
		
	PageCallBack(){
		id = 0;
	}
	public void foundURI(FreenetURI uri){
		//now we have the id of the page that had called this link
		queueURI(uri);
		int iduri = (Integer) uriIds.get(uri);
		Vector outlink = (Vector) outlinks.get(id);
		if(!outlink.contains(iduri))	
			outlink.add(iduri);
		outlinks.remove(id);
		outlinks.put(id, outlink);
		try{
		FileWriter out = new FileWriter("outlink",true);
		out.write(" id "+id+" size "+ outlink.size()+" \n");
		out.close();
		}catch(Exception e){}

		if(inlinks.containsKey(iduri)){
			Vector inlink = (Vector) inlinks.get(iduri);
			try{
				FileWriter out = new FileWriter("inlink",true);
				out.write(" id "+iduri+" size "+ inlink.size()+" \n");
				out.close();
				}catch(Exception e){}
		
			if(!inlink.contains(id)) inlink.add(id);
			inlinks.remove(iduri);
			inlinks.put(iduri, inlink);
			
		}
		startSomeRequests();
	}
	public void onText(String s, String type, URI baseURI){
		try{
			FileWriter outp = new FileWriter("ontext",true);
			outp.write("inside on text with id"+id+" \n");
			outp.close();
		}catch(Exception e){}
//		FreenetURI uri;
//		try {
//			uri = new FreenetURI(baseURI.getPath().substring(1));
//		} catch (MalformedURLException e) {
//			Logger.error(this, "Caught " + e, e);
//			return;
//		}
		 
		
      
		if((type != null) && (type.length() != 0) && type.toLowerCase().equals("title")
		   && (s != null) && (s.length() != 0) && (s.indexOf('\n') < 0)) {
			/* We should have a correct title */
		//	titlesOfURIs.put(uri.toString(), s);
			titlesOfIds.put(id, s);
			
			type = "title";
			
		}
		else type = null;


		String[] words = s.split("[^A-Za-z0-9]");

		Integer lastPosition = null;

		//lastPosition = (Integer)lastPositionByURI.get(uri.toString());
		lastPosition = (Integer)lastPositionById.get(id);
		if(lastPosition == null)
			lastPosition = new Integer(1); /* We start to count from 1 */

		for (int i = 0; i < words.length; i++) {
			String word = words[i];
			if ((word == null) || (word.length() == 0))
				continue;
			word = word.toLowerCase();
			try{
			if(type == null)
				addWord(word, lastPosition.intValue() + i, id);
			else
				addWord(word, -1 * (i+1), id);
			}
			catch (Exception e){}
		}
		
		if(type == null) {
			lastPosition = new Integer(lastPosition.intValue() + words.length);
		//	lastPositionByURI.put(uri.toString(), lastPosition);
			lastPositionById.put(id, lastPosition);
		}
		
	}
private synchronized void addWord(String word, int position,int id) throws Exception{
		
		
		if(word.length() < 3)
			return;
		
		//word = word.intern();


		//FreenetURI[] uris = (FreenetURI[]) urisByWord.get(word);
		Integer[] ids = (Integer[]) idsByWord.get(word);
		
	//	urisWithWords.add(uri);
		idsWithWords.add(id);
		try{
			FileWriter outp = new FileWriter("addWord",true);
			outp.write("ID ="+id+" uri ="+idUris.get(id)+"\n");
			outp.close();
		}catch(Exception e){}
//	FileWriter outp = new FileWriter("uricheck",true);
//	outp.write(uri.getDocName()+"\n");
//	outp.write(uri.getKeyType()+"\n");
//	outp.write(uri.getMetaString()+"\n");
//	outp.write(uri.getGuessableKey()+"\n");
//	outp.write(uri.hashCode()+"\n");
//	outp.write(uri.getPreferredFilename()+"\n");
//	
//	outp.close();

		/* Word position indexation */
		HashMap wordPositionsForOneUri = (HashMap)positionsByWordById.get(id); /* For a given URI, take as key a word, and gives position */
		
		if(wordPositionsForOneUri == null) {
			wordPositionsForOneUri = new HashMap();
			wordPositionsForOneUri.put(word, new Integer[] { new Integer(position) });
			//positionsByWordByURI.put(uri.toString(), wordPositionsForOneUri);
			positionsByWordById.put(id, wordPositionsForOneUri);
		} else {
			Integer[] positions = (Integer[])wordPositionsForOneUri.get(word);

			if(positions == null) {
				positions = new Integer[] { new Integer(position) };
				wordPositionsForOneUri.put(word, positions);
			} else {
				Integer[] newPositions = new Integer[positions.length + 1];

				System.arraycopy(positions, 0, newPositions, 0, positions.length);
				newPositions[positions.length] = new Integer(position);

				wordPositionsForOneUri.put(word, newPositions);
			}
		}
	
		if (ids == null) {
			idsByWord.put(word, new Integer[] { id });
			
		} else {
			for (int i = 0; i < ids.length; i++) {
				if (ids[i].equals(id))
					return;
			}
			Integer[] newIDs = new Integer[ids.length + 1];
			System.arraycopy(ids, 0, newIDs, 0, ids.length);
			newIDs[ids.length] = id;
			idsByWord.put(word, newIDs);
		}
		//the new word is added here in urisByWord
		tMap.put(MD5(word), word);
		long time_indexing = System.currentTimeMillis();
		if (tProducedIndex + minTimeBetweenEachIndexRewriting * 10 < System.currentTimeMillis()) {
			try {
				//produceIndex();
				//check();
				
				if(indexing){
				generateIndex2();
				produceIndex2();
				if((System.currentTimeMillis() - time_indexing)/(System.currentTimeMillis() - tProducedIndex) > MAX_TIME_SPENT_INDEXING) indexing= false;
				else indexing = true;
				}
				
			} catch (IOException e) {
				Logger.error(this, "Caught " + e + " while creating index", e);
			}
			tProducedIndex = System.currentTimeMillis();
		}
		
	}
	
}
public String handleHTTPPut(HTTPRequest request) throws PluginHTTPException{
	return null;
}
public String handleHTTPPost(HTTPRequest request) throws PluginHTTPException{
	return null;
}

public void onFoundEdition(long l, USK key){
	FreenetURI uri = key.getURI();
	if(runningFetchesByURI.containsKey(uri)) runningFetchesByURI.remove(uri);
	uri = key.getURI().setSuggestedEdition(l);
	queueURI(uri);
}
	
	
}
