/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package plugins.XMLSpider;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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

import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
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
import freenet.clients.http.ToadletContext;
import freenet.clients.http.ToadletContextClosedException;
import freenet.clients.http.filter.ContentFilter;
import freenet.clients.http.filter.FoundURICallback;
import freenet.clients.http.filter.UnsafeContentTypeException;
import freenet.keys.FreenetURI;
import freenet.keys.USK;
import freenet.node.NodeClientCore;
import freenet.node.PrioRunnable;
import freenet.node.RequestStarter;
import freenet.pluginmanager.FredPlugin;
import freenet.pluginmanager.FredPluginHTTP;
import freenet.pluginmanager.FredPluginHTTPAdvanced;
import freenet.pluginmanager.FredPluginThreadless;
import freenet.pluginmanager.PluginHTTPException;
import freenet.pluginmanager.PluginRespirator;
import freenet.support.Logger;
import freenet.support.api.Bucket;
import freenet.support.api.HTTPRequest;
import freenet.support.io.NativeThread;
import freenet.support.io.NullBucketFactory;

/**
 * XMLSpider. Produces xml index for searching words. 
 * In case the size of the index grows up a specific threshold the index is split into several subindices.
 * The indexing key is the md5 hash of the word.
 * 
 *  @author swati goyal
 *  
 */
public class XMLSpider implements FredPlugin, FredPluginHTTP, FredPluginThreadless,  FredPluginHTTPAdvanced, USKCallback {

	long tProducedIndex;
	/**
	 * Stores the found words along with md5
	 */
	public TreeMap tMap = new TreeMap();
	int count;
	// URIs visited, or fetching, or queued. Added once then forgotten about.
	/**
	 * 
	 * Lists the uris that have been vistied by the spider
	 */
	public final HashSet visitedURIs = new HashSet();
	private final HashSet idsWithWords = new HashSet();
	/**
	 * 
	 * Lists the uris that were visited but failed.
	 */
	public final HashSet failedURIs = new HashSet();

	private final HashSet queuedURISet = new HashSet();
	/**
	 * 
	 * Lists the uris that are still queued.
	 * 
	 * Since we have limited RAM, and we don't want stuff to be on the cooldown queue for a 
	 * long period, we use 2 retries (to stay off the cooldown queue), and we go over the queued
	 * list 3 times for each key.
	 */
	public final LinkedList[] queuedURIList = new LinkedList[] { new LinkedList(), new LinkedList(), new LinkedList() };
	private final HashMap runningFetchesByURI = new HashMap();

	private final HashMap idsByWord = new HashMap();

	private final HashMap titlesOfIds = new HashMap();
	private final HashMap uriIds = new HashMap();
	private final HashMap idUris = new HashMap();
	
	// Re-enable outlinks/inlinks when we publish them or use them for ranking.
	/**
	 * Lists the outlinks from a particular page, 
	 * </br> indexed by the id of page uri
	 */
//	public final HashMap outlinks = new HashMap();
	/**
	 * Lists the inlinks to a particular page,
	 *  indexed by the id of page uri.
	 */
//	public final HashMap inlinks = new HashMap();
	private Vector indices;
	private int match;
	private Integer id;
	private long time_taken;
/*
 * minTimeBetweenEachIndexRewriting in seconds 
 */
	private static final int minTimeBetweenEachIndexRewriting = 600;
	/**
	 * directory where the generated indices are stored. 
	 * Needs to be created before it can be used
	 */
	public static final String DEFAULT_INDEX_DIR = "myindex7/";
	/**
	 * Lists the allowed mime types of the fetched page. 
	 */
	public Set allowedMIMETypes;
	private static final int MAX_ENTRIES = 2000;
	private static final long MAX_SUBINDEX_UNCOMPRESSED_SIZE = 4*1024*1024;
	private static int version = 32;
	private static final String pluginName = "XML spider "+version;
	/**
	 * Gives the allowed fraction of total time spent on generating indices with
	 * maximum value = 1; minimum value = 0. 
	 */
	public static final double MAX_TIME_SPENT_INDEXING = 0.5;

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

	private NodeClientCore core;
	private FetchContext ctx;
	// Equal to Frost, ARK fetches etc. One step down from Fproxy.
	// Any lower makes it very difficult to debug. Maybe reduce for production - after solving the ARK bugs.
	private final short PRIORITY_CLASS = RequestStarter.IMMEDIATE_SPLITFILE_PRIORITY_CLASS;
	private boolean stopped = true;
	PluginRespirator pr;

	/**
	 * Adds the found uri to the list of to-be-retrieved uris. <p>Every usk uri added as ssk.
	 * @param uri the new uri that needs to be fetched for further indexing
	 */
	public synchronized void queueURI(FreenetURI uri) {
		if((uri.getKeyType()).equals("USK")){
			if(uri.getSuggestedEdition() < 0)
				uri = uri.setSuggestedEdition((-1)* uri.getSuggestedEdition());
			try{
				uri = ((USK.create(uri)).getSSK()).getURI();
				(ctx.uskManager).subscribe(USK.create(uri),this, false, this);	
			}
			catch(Exception e){}
		}

		if ((!visitedURIs.contains(uri)) && queuedURISet.add(uri)) {
			queuedURIList[0].addLast(uri);
			visitedURIs.add(uri);
			uriIds.put(uri, id);
			idUris.put(id, uri);
			id = new Integer(id.intValue()+1);
		}
	}

	private void startSomeRequests() {


		FreenetURI[] initialURIs = core.getBookmarkURIs();
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
			int queued = queuedURIList[0].size() + queuedURIList[1].size() + queuedURIList[2].size();

			if ((running >= maxParallelRequests) || (queued == 0))
				return;

			toStart = new ArrayList(Math.min(maxParallelRequests - running, queued));

			for (int i = running; i < maxParallelRequests; i++) {
				boolean found = false;
				for(int j=0;j<queuedURIList.length;j++) {
					if(queuedURIList[j].isEmpty()) continue;
					FreenetURI uri = (FreenetURI) queuedURIList[j].removeFirst();
					if(j == queuedURIList.length) queuedURISet.remove(uri);
					ClientGetter getter = makeGetter(uri, j);
					toStart.add(getter);
					found = true;
					break;
				}
				if(!found) break;
			}
		}
		for (int i = 0; i < toStart.size(); i++) {

			ClientGetter g = (ClientGetter) toStart.get(i);
			try {
				runningFetchesByURI.put(g.getURI(), g);
				g.start();
			} catch (FetchException e) {
				onFailure(e, g, ((MyClientCallback)g.getClientCallback()).tries);
			}
		}
	}

	private final ClientCallback[] clientCallbacks =
		new ClientCallback[] {
			new MyClientCallback(0),
			new MyClientCallback(1),
			new MyClientCallback(2)
	};

	private class MyClientCallback implements ClientCallback {

		final int tries;
		
		public MyClientCallback(int x) {
			tries = x;
			// TODO Auto-generated constructor stub
		}

		public void onFailure(FetchException e, ClientGetter state) {
			XMLSpider.this.onFailure(e, state, tries);
		}

		public void onFailure(InsertException e, BaseClientPutter state) {
			// Ignore
		}

		public void onFetchable(BaseClientPutter state) {
			// Ignore
		}

		public void onGeneratedURI(FreenetURI uri, BaseClientPutter state) {
			// Ignore
		}

		public void onMajorProgress() {
			// Ignore
		}

		public void onSuccess(FetchResult result, ClientGetter state) {
			XMLSpider.this.onSuccess(result, state);
		}

		public void onSuccess(BaseClientPutter state) {
			// Ignore
		}
		
	}
	
	private ClientGetter makeGetter(FreenetURI uri, int retries) {
		ClientGetter g = new ClientGetter(clientCallbacks[retries], core.requestStarters.chkFetchScheduler, core.requestStarters.sskFetchScheduler, uri, ctx, PRIORITY_CLASS, this, null, null);
		return g;
	}
	/**
	 * Processes the successfully fetched uri for further outlinks.
	 * 
	 * @param result
	 * @param state
	 */
	public void onSuccess(FetchResult result, ClientGetter state) {
		FreenetURI uri = state.getURI();

		try {
		
			ClientMetadata cm = result.getMetadata();
			Bucket data = result.asBucket();
			String mimeType = cm.getMIMEType();
			
			Integer id;
			synchronized(this) {
				sizeOfURIs.put(uri.toString(), new Long(data.size()));
				mimeOfURIs.put(uri.toString(), mimeType);
				id = (Integer) uriIds.get(uri);
//				inlinks.put(page.id, new Vector());
//				outlinks.put(page.id, new Vector());
			}
			/*
			 * instead of passing the current object, the pagecallback object for every page is passed to the content filter
			 * this has many benefits to efficiency, and allows us to identify trivially which page is being indexed.
			 * (we CANNOT rely on the base href provided).
			 */
			PageCallBack page = new PageCallBack(id);
			Logger.minor(this, "Successful: "+uri+" : "+page.id);
			
			try {
				Logger.minor(this, "Filtering "+uri+" : "+page.id);
				ContentFilter.filter(data, new NullBucketFactory(), mimeType, uri.toURI("http://127.0.0.1:8888/"), page);
			} catch (UnsafeContentTypeException e) {
				return; // Ignore
			} catch (IOException e) {
				Logger.error(this, "Bucket error?: " + e, e);
			} catch (URISyntaxException e) {
				Logger.error(this, "Internal error: " + e, e);
			} finally {
				data.free();
			}
		} finally {
			synchronized (this) {
				runningFetchesByURI.remove(uri);
			}
			startSomeRequests();
		}
	}

	public void onFailure(FetchException e, ClientGetter state, int tries) {
		FreenetURI uri = state.getURI();
		Logger.minor(this, "Failed: "+uri+" : "+e);

		synchronized (this) {
			runningFetchesByURI.remove(uri);
			failedURIs.add(uri);
			tries++;
			if(tries < queuedURIList.length && !e.isFatal())
				queuedURIList[tries].addLast(uri);
		}
		if (e.newURI != null)
			queueURI(e.newURI);

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

		if (idsByWord.isEmpty() || idsWithWords.isEmpty()) {
			System.out.println("No URIs with words");
			return;
		}
		//the main index file 
		File outputFile = new File(DEFAULT_INDEX_DIR+"index.xml");
		// Use a stream so we can explicitly close - minimise number of filehandles used.
		BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(outputFile));
		StreamResult resultStream;
		resultStream = new StreamResult(fos);

		try {
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

			Logger.error(this, "Spider: Error while initializing XML generator: "+e.toString(), e);
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
		/*
		 * the max number of digits in md5 to be used for matching with the search query is stored in the xml
		 */
		Element prefixElement = xmlDoc.createElement("prefix");
		/* Adding word index */
		Element keywordsElement = xmlDoc.createElement("keywords");
		for(int i = 0;i<indices.size();i++){

			Element subIndexElement = xmlDoc.createElement("subIndex");
			subIndexElement.setAttribute("key", (String) indices.elementAt(i));
			//the subindex element key will contain the bits used for matching in that subindex
			keywordsElement.appendChild(subIndexElement);
		}

		prefixElement.setAttribute("value",match+"");
		rootElement.appendChild(prefixElement);
		rootElement.appendChild(headerElement);
		rootElement.appendChild(keywordsElement);

		/* Serialization */
		DOMSource domSource = new DOMSource(xmlDoc);
		TransformerFactory transformFactory = TransformerFactory.newInstance();
		Transformer serializer;

		try {
			serializer = transformFactory.newTransformer();
		} catch(javax.xml.transform.TransformerConfigurationException e) {
			Logger.error(this, "Spider: Error while serializing XML (transformFactory.newTransformer()): "+e.toString(), e);
			return;
		}

		serializer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		serializer.setOutputProperty(OutputKeys.INDENT,"yes");

		/* final step */
		try {
			serializer.transform(domSource, resultStream);
		} catch(javax.xml.transform.TransformerException e) {
			Logger.error(this, "Spider: Error while serializing XML (transform()): "+e.toString(), e);
			return;
		}
		} finally {
			fos.close();
		}

		if(Logger.shouldLog(Logger.MINOR, this))
			Logger.minor(this, "Spider: indexes regenerated - tProducedIndex="+(System.currentTimeMillis()-tProducedIndex)+"ms ago time taken="+time_taken+"ms");

		//The main xml file is generated 
		//As each word is generated enter it into the respective subindex
		//The parsing will start and nodes will be added as needed 

	}
	/**
	 * Generates the subindices. 
	 * Each index has less than {@code MAX_ENTRIES} words.
	 * The original treemap is split into several sublists indexed by the common substring
	 * of the hash code of the words
	 * @throws Exception
	 */
	private synchronized void generateIndex2() throws Exception{
		Logger.normal(this, "Generating index...");
		//using the tMap generate the xml indices
		if (idsByWord.isEmpty() || idsWithWords.isEmpty()) {
			System.out.println("No URIs with words");
			return;
		}

		indices = new Vector();
		int prefix = 1;
		match = 1;
		Vector list = new Vector();
		Iterator it = tMap.keySet().iterator();

		String str = (String) it.next();
		int i = 0;
		while(it.hasNext())
		{
			String key =(String) it.next();
			//create a list of the words to be added in the same subindex
			if(key.substring(0, prefix).equals(str.substring(0, prefix))) 
			{i++;
			list.add(key);
			}
			else {
				//generate the appropriate subindex with the current list
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
		boolean logMINOR = Logger.shouldLog(Logger.MINOR, this);
		/*
		 * if the list is less than max allowed entries in a file then directly generate the xml 
		 * otherwise split the list into further sublists
		 * and iterate till the number of entries per subindex is less than the allowed value
		 */
		if(logMINOR)
			Logger.minor(this, "Generating subindex for "+list.size()+" entries with prefix length "+p);

		try {
			if(list.size() < MAX_ENTRIES)
			{	
				generateXML(list,p);
				return;
			}
		} catch (TooBigIndexException e) {
			// Handle below
		}
		if(logMINOR)
			Logger.minor(this, "Too big subindex for "+list.size()+" entries with prefix length "+p);
			//prefix needs to be incremented
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
					i++;
				}
				else {
					generateSubIndex(prefix,subVector(list,index,i-1));
					index = i;
					str = key;
				}
			}
			generateSubIndex(prefix,subVector(list,index,i-1));
	}	

	private class TooBigIndexException extends Exception {
		private static final long serialVersionUID = -6172560811504794914L;
	}
	
	/**
	 * generates the xml index with the given list of words with prefix number of matching bits in md5
	 * @param list  list of the words to be added in the index
	 * @param prefix number of matching bits of md5
	 * @throws Exception
	 */
	public synchronized void generateXML (Vector list, int prefix) throws TooBigIndexException, Exception
	{
		String p = ((String) list.elementAt(0)).substring(0, prefix);
		indices.add(p);
		File outputFile = new File(DEFAULT_INDEX_DIR+"index_"+p+".xml");
		BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(outputFile));
		StreamResult resultStream;
		resultStream = new StreamResult(fos);

		try {
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
			Logger.error(this, "Spider: Error while initializing XML generator: "+e.toString(), e);
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
		EntriesElement.setAttribute("value", list.size()+"");

		/* Adding word index */
		Element keywordsElement = xmlDoc.createElement("keywords");
		Vector fileid = new Vector();
		for(int i =0;i<list.size();i++)
		{
			Element wordElement = xmlDoc.createElement("word");
			String str = (String) tMap.get(list.elementAt(i));
			wordElement.setAttribute("v",str );
			Integer[] idsForWord = (Integer[]) idsByWord.get(str);
			for (int j = 0; j < idsForWord.length; j++) {
				Integer id = idsForWord[j];
				Integer x = id;
				if (x == null) {
					Logger.error(this, "Eh?");
					continue;
				}
				/*
				 * adding file information
				 * uriElement - lists the id of the file containing a particular word
				 * fileElement - lists the id,key,title of the files mentioned in the entire subindex
				 */
				Element uriElement = xmlDoc.createElement("file");
				Element fileElement = xmlDoc.createElement("file");
				uriElement.setAttribute("id", x.toString());
				fileElement.setAttribute("id", x.toString());
				fileElement.setAttribute("key",(idUris.get(id)).toString());
				if(titlesOfIds.containsKey(id))
					fileElement.setAttribute("title",(titlesOfIds.get(id)).toString());
				else 
					fileElement.setAttribute("title",(idUris.get(id)).toString());
				
				/* Position by position */

				HashMap positionsForGivenWord = (HashMap)positionsByWordById.get(x);
				Integer[] positions = (Integer[])positionsForGivenWord.get(str);
				StringBuilder positionList = new StringBuilder();

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
			Logger.error(this, "Spider: Error while serializing XML (transformFactory.newTransformer()): "+e.toString(), e);
			return;
		}
		serializer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		serializer.setOutputProperty(OutputKeys.INDENT,"yes");
		/* final step */
		try {
			serializer.transform(domSource, resultStream);
		} catch(javax.xml.transform.TransformerException e) {
			Logger.error(this, "Spider: Error while serializing XML (transform()): "+e.toString(), e);
			return;
		}
		} finally {
			fos.close();
		}
		if(outputFile.length() > MAX_SUBINDEX_UNCOMPRESSED_SIZE && list.size() > 1) {
			outputFile.delete();
			throw new TooBigIndexException();
		}

		if(Logger.shouldLog(Logger.MINOR, this))
			Logger.minor(this, "Spider: indexes regenerated.");
	}


	public void handleGet(HTTPRequest request, ToadletContext context) throws IOException, ToadletContextClosedException {
		/*
		 * ignore
		 */
	}


	public void handlePost(HTTPRequest request, ToadletContext context) throws IOException {
		/*
		 * ignore
		 */
	}

	/**
	 * @see freenet.oldplugins.plugin.Plugin#getPluginName()
	 */
	public String getPluginName() {
		return pluginName;
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
			for(int i=0;i<queuedURIList.length;i++)
				queuedURIList[i].clear();
		}
	}

	public void onMajorProgress() {
		// Ignore
	}

	public void onFetchable(BaseClientPutter state) {
		// Ignore
	}
	private static String convertToHex(byte[] data) {
		StringBuilder buf = new StringBuilder();
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

	/*
	 * calculate the md5 for a given string
	 */
	private static String MD5(String text) throws NoSuchAlgorithmException, UnsupportedEncodingException  {
		MessageDigest md;
		md = MessageDigest.getInstance("MD5");
		byte[] md5hash = new byte[32];
		md.update(text.getBytes("iso-8859-1"), 0, text.length());
		md5hash = md.digest();
		return convertToHex(md5hash);
	}

	public void generateSubIndex(String filename){
//		generates the new subIndex
		File outputFile = new File(filename);
		BufferedOutputStream fos;
		try {
			fos = new BufferedOutputStream(new FileOutputStream(outputFile));
		} catch (FileNotFoundException e1) {
			Logger.error(this, "Cannot open "+filename+" writing index : "+e1, e1);
			return;
		}
		try {
		StreamResult resultStream;
		resultStream = new StreamResult(fos);

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
			Logger.error(this, "Spider: Error while initializing XML generator: "+e.toString(), e);
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
			Logger.error(this, "Spider: Error while serializing XML (transformFactory.newTransformer()): "+e.toString(), e);
			return;
		}


		serializer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		serializer.setOutputProperty(OutputKeys.INDENT,"yes");

		/* final step */
		try {
			serializer.transform(domSource, resultStream);
		} catch(javax.xml.transform.TransformerException e) {
			Logger.error(this, "Spider: Error while serializing XML (transform()): "+e.toString(), e);
			return;
		}
		} finally {
			try {
				fos.close();
			} catch (IOException e) {
				// Ignore
			}
		}

		if(Logger.shouldLog(Logger.MINOR, this))
			Logger.minor(this, "Spider: indexes regenerated.");
	}

	public void terminate(){
		synchronized (this) {
			stopped = true;
			for(int i=0;i<queuedURIList.length;i++)
				queuedURIList[i].clear();
		}
	}

	public void runPlugin(PluginRespirator pr){
		this.pr = pr;
		this.id = new Integer(0);
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

		ctx.allowedMIMETypes = new HashSet(allowedMIMETypes);

		tProducedIndex = System.currentTimeMillis();
		stopped = false;
		count = 0;
		try{
		Runtime.getRuntime().exec("mkdir "+DEFAULT_INDEX_DIR);
		}
		catch(Exception e){
			Logger.error(this, "Could not create default index directory "+e.toString(), e);
		}
		//startPlugin();
		pr.getNode().executor.execute(new Runnable() {
			public void run() {
				try{
					Thread.sleep(30 * 1000); // Let the node start up
				} catch (InterruptedException e){}
				startSomeRequests();
				scheduleMakeIndex();
			}
		}, "Spider Plugin Starter");
	}

	/**
	 * Interface to the Spider data
	 */
	public String handleHTTPGet(HTTPRequest request) throws PluginHTTPException{
		StringBuilder out = new StringBuilder();

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
/*
 * List the visited, queued, failed and running fetches on the web interface
 */
	private synchronized void appendList(String listname, StringBuilder out, String stylesheet)
	{
		Iterator it = (runningFetchesByURI.keySet()).iterator();
		if(listname.equals("running"))
			it = (runningFetchesByURI.keySet()).iterator();
		if(listname.equals("visited"))
			it = (new HashSet(visitedURIs)).iterator();
		if(listname.startsWith("queued"))
			it = (new ArrayList(queuedURIList[Integer.parseInt(listname.substring("queued".length()))])).iterator();
		if(listname.equals("failed"))
			it = (new HashSet(failedURIs)).iterator();
		while(it.hasNext())
			out.append("<code>"+it.next().toString()+"</code><br/>");
	}

	private void appendDefaultPageStart(StringBuilder out, String stylesheet) {

		out.append("<HTML><HEAD><TITLE>" + pluginName + "</TITLE>");
		if(stylesheet != null)
			out.append("<link href=\""+stylesheet+"\" type=\"text/css\" rel=\"stylesheet\" />");
		out.append("</HEAD><BODY>\n");
		out.append("<CENTER><H1>" + pluginName + "</H1><BR/><BR/><BR/>\n");
		out.append("Add uri:");
		out.append("<form method=\"GET\"><input type=\"text\" name=\"adduri\" /><br/><br/>");
		out.append("<input type=\"submit\" value=\"Add uri\" /></form>");
		Set runningFetches;
		Set visited;
		Set failed;
		List[] queued = new List[queuedURIList.length];
		synchronized(this) {
			visited = new HashSet(visitedURIs);
			failed = new HashSet(failedURIs);
			for(int i=0;i<queuedURIList.length;i++)
				queued[i] = new ArrayList(queuedURIList[i]);
			runningFetches = new HashSet(runningFetchesByURI.keySet());
		}
		out.append("<p><h3>Running Fetches</h3></p>");
		out.append("<br/>Size :"+runningFetches.size()+"<br/>");
		appendList(runningFetches,out,stylesheet);
		out.append("<p><a href=\"?list="+"running"+"\">Show all</a><br/></p>");
		for(int j=0;j<queued.length;j++) {
			out.append("<p><h3>Queued URIs ("+j+")</h3></p>");
			out.append("<br/>Size :"+queued[j].size()+"<br/>");
			int i = 0;
			Iterator it=queued[j].iterator();
			while(it.hasNext()){
				if(i<=maxShownURIs){
					out.append("<code>"+it.next().toString()+"</code><br/>");
				}
				else break;
				i++;
			}
			out.append("<p><a href=\"?list="+"queued"+j+"\">Show all</a><br/></p>");
		}
		out.append("<p><h3>Visited URIs</h3></p>");
		out.append("<br/>Size :"+visited.size()+"<br/>");
		appendList(visited,out,stylesheet);
		out.append("<p><a href=\"?list="+"visited"+"\">Show all</a><br/></p>");
		out.append("<p><h3>Failed URIs</h3></p>");
		out.append("<br/>Size :"+failed.size()+"<br/>");
		appendList(failed,out,stylesheet);
		out.append("<p><a href=\"?list="+"failed"+"\">Show all</a><br/></p>");
		out.append("<p>Time taken in generating index = "+time_taken+"</p>");
	}


	private void appendDefaultHeader(StringBuilder out, String stylesheet){
		out.append("<HTML><HEAD><TITLE>" + pluginName + "</TITLE>");
		if(stylesheet != null)
			out.append("<link href=\""+stylesheet+"\" type=\"text/css\" rel=\"stylesheet\" />");
		out.append("</HEAD><BODY>\n");
		out.append("<CENTER><H1>" + pluginName + "</H1><BR/><BR/><BR/>\n");
		out.append("Add uri:");
		out.append("<form method=\"GET\"><input type=\"text\" name=\"adduri\" /><br/><br/>");
		out.append("<input type=\"submit\" value=\"Add uri\" /></form>");
	}


	private void appendList(Set  list,StringBuilder out, String stylesheet){
		Iterator it = list.iterator();
		int i = 0;
		while(it.hasNext()){
			if(i<=maxShownURIs){
				out.append("<code>"+it.next().toString()+"</code><br/>");
			}
			else{
				break;
			}
			i++;
		}
	}

	/**
	 * creates the callback object for each page.
	 *<p>Used to create inlinks and outlinks for each page separately.
	 * @author swati
	 *
	 */
	public class PageCallBack implements FoundURICallback{
		final Integer id;
		/*
		 * id of the page as refrenced in uriIds
		 */	
		PageCallBack(Integer i){
			id = i;
		}

		public void foundURI(FreenetURI uri){
			// Ignore
		}
		
		public void foundURI(FreenetURI uri, boolean inline){

			Logger.minor(this, "foundURI "+uri+" on "+id);
			queueURI(uri);
			// FIXME re-enable outlinks/inlinks when we can do something useful with them
//			synchronized(XMLSpider.this) {
//			Integer iduri = (Integer) uriIds.get(uri);
/*
 * update the outlink information for the current page
 */
//			if(outlinks.containsKey(id)){
//				Vector outlink = (Vector) outlinks.get(id);
//				if(!outlink.contains(iduri))	
//					outlink.add(iduri);
//				outlinks.remove(id);
//				outlinks.put(id, outlink);
//			}
//			else 
//			{
//				Vector outlink = new Vector();
//				outlink.add(iduri);
//				outlinks.put(id, outlink);
//			}
/*
 * update the inlink information for the new link 
 */
//			if(inlinks.containsKey(iduri)){
//				Vector inlink = (Vector) inlinks.get(iduri);
//				if(!inlink.contains(id)) inlink.add(id);
//				inlinks.remove(iduri);
//				inlinks.put(iduri, inlink);
//			}
//			else 
//			{
//				Vector inlink = new Vector();
//				inlink.add(id);
//				inlinks.put(iduri, inlink);
//			}
//			} // synchronized
			
			startSomeRequests();
		}


		public void onText(String s, String type, URI baseURI){
			
			Logger.minor(this, "onText on "+id+" ("+baseURI+")");

			if((type != null) && (type.length() != 0) && type.toLowerCase().equals("title")
					&& (s != null) && (s.length() != 0) && (s.indexOf('\n') < 0)) {
				/*
				 * title of the page 
				 */
				titlesOfIds.put(id, s);
				type = "title";
			}
			else type = null;
			/*
			 * determine the position of the word in the retrieved page
			 */
			String[] words = s.split("[^A-Za-z0-9]");
			Integer lastPosition = null;
			lastPosition = (Integer)lastPositionById.get(id);

			if(lastPosition == null)
				lastPosition = new Integer(1); 
			for (int i = 0; i < words.length; i++) {
				String word = words[i];
				if ((word == null) || (word.length() == 0))
					continue;
				word = word.toLowerCase();
				word = word.intern();
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
				lastPositionById.put(id, lastPosition);
			}

		}

		private void addWord(String word, int position,Integer id) throws Exception{
			synchronized(XMLSpider.this) {
			if(word.length() < 3)
				return;

			Integer[] ids = (Integer[]) idsByWord.get(word);
			idsWithWords.add(id);

			/* Word position indexation */
			HashMap wordPositionsForOneUri = (HashMap)positionsByWordById.get(id); /* For a given URI, take as key a word, and gives position */
			if(wordPositionsForOneUri == null) {
				wordPositionsForOneUri = new HashMap();
				wordPositionsForOneUri.put(word, new Integer[] { new Integer(position) });
				positionsByWordById.put(id, wordPositionsForOneUri);
			} 
			else {
				Integer[] positions = (Integer[])wordPositionsForOneUri.get(word);
				if(positions == null) {
					positions = new Integer[] { new Integer(position) };
					wordPositionsForOneUri.put(word, positions);
				} 
				else {
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

			tMap.put(MD5(word), word);
			//long time_indexing = System.currentTimeMillis();
//			FileWriter outp = new FileWriter("logfile",true);
			mustWriteIndex = true;
			}
		}
	}

	private boolean mustWriteIndex = false;
	
	public void makeIndex() throws Exception {
		try {
			synchronized(this) {
				if(!mustWriteIndex) {
					if(Logger.shouldLog(Logger.MINOR, this)) Logger.minor(this, "Not making index, no data added since last time");
					return;
				}
				mustWriteIndex = false;
			}
		time_taken = System.currentTimeMillis();
		generateIndex2();
		if(Logger.shouldLog(Logger.MINOR, this)) Logger.minor(this, "Producing top index...");
		produceIndex2();
		time_taken = System.currentTimeMillis() - time_taken;
		tProducedIndex = System.currentTimeMillis();
		if(Logger.shouldLog(Logger.MINOR, this)) Logger.minor(this, "Made index, took "+time_taken);
		} finally {
			scheduleMakeIndex();
		}
	}

	private void scheduleMakeIndex() {
		core.getTicker().queueTimedJob(new PrioRunnable() {

			public void run() {
				try {
					makeIndex();
				} catch (Exception e) {
					Logger.error(this, "Could not generate index: "+e, e);
				}
			}

			public int getPriority() {
				return NativeThread.LOW_PRIORITY;
			}
			
		}, minTimeBetweenEachIndexRewriting * 1000);
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

	public short getPollingPriorityNormal() {
		return (short) Math.min(RequestStarter.MINIMUM_PRIORITY_CLASS, PRIORITY_CLASS + 1);
	}

	public short getPollingPriorityProgress() {
		return PRIORITY_CLASS;
	}

	public boolean persistent() {
		return false;
	}
}
