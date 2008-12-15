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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

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

import com.db4o.Db4o;
import com.db4o.ObjectContainer;
import com.db4o.ObjectSet;
import com.db4o.config.Configuration;
import com.db4o.config.QueryEvaluationMode;
import com.db4o.diagnostic.DiagnosticToConsole;
import com.db4o.query.Query;

import freenet.client.ClientMetadata;
import freenet.client.FetchContext;
import freenet.client.FetchException;
import freenet.client.FetchResult;
import freenet.client.InsertException;
import freenet.client.async.BaseClientPutter;
import freenet.client.async.ClientCallback;
import freenet.client.async.ClientGetter;
import freenet.client.async.USKCallback;
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
import freenet.pluginmanager.FredPluginVersioned;
import freenet.pluginmanager.PluginHTTPException;
import freenet.pluginmanager.PluginRespirator;
import freenet.support.HTMLEncoder;
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
public class XMLSpider implements FredPlugin, FredPluginHTTP, FredPluginThreadless, FredPluginVersioned,
        FredPluginHTTPAdvanced, USKCallback {
	static enum Status {
		/** For simplicity, running is also mark as QUEUED */
		QUEUED, SUCCEEDED, FAILED
	};
	
	static class Page {
		/** Page Id */
		long id;
		/** URI of the page */
		String uri;
		/** Title */
		String pageTitle;
		/** Status */
		Status status = Status.QUEUED;
		/** Last Change Time */
		long lastChange = System.currentTimeMillis();
		/** Comment, for debugging */
		String comment;

		public Page() {}	// for db4o callConstructors(true)

		@Override
		public int hashCode() {
			return (int) (id ^ (id >>> 32));
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;

			return id == ((Page) obj).id;
		}

		@Override
		public String toString() {
			return "[PAGE: id=" + id + ", title=" + pageTitle + ", uri=" + uri + ", status=" + status + ", comment="
			        + comment
			+ "]";
		}
	}

	static class Term {
		/** MD5 of the term */
		String md5;
		/** Term */
		String word;

		public Term(String word) {
			this.word = word;
			md5 = MD5(word);
		}

		public Term() {
		}
	}
	
	/** Document ID of fetching documents */
	protected Map<Page, ClientGetter> runningFetch = new HashMap<Page, ClientGetter>();

	long tProducedIndex;
	protected AtomicLong maxPageId;
	
	private final HashMap<String, Long[]> idsByWord = new HashMap<String, Long[]>();
	
	private Vector<String> indices;
	private int match;
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
	public Set<String> allowedMIMETypes;
	private static final int MAX_ENTRIES = 800;
	private static final long MAX_SUBINDEX_UNCOMPRESSED_SIZE = 4*1024*1024;
	private static int version = 33;
	private static final String pluginName = "XML spider " + version;

	public String getVersion() {
		return version + " r" + Version.getSvnRevision();
	}

	/**
	 * Gives the allowed fraction of total time spent on generating indices with
	 * maximum value = 1; minimum value = 0. 
	 */
	public static final double MAX_TIME_SPENT_INDEXING = 0.5;

	private static final String indexTitle= "XMLSpider index";
	private static final String indexOwner = "Freenet";
	private static final String indexOwnerEmail = null;
	
//	private final HashMap lastPositionByURI = new HashMap(); /* String (URI) -> Integer */ /* Use to determine word position on each uri */
//	private final HashMap positionsByWordByURI = new HashMap(); /* String (URI) -> HashMap (String (word) -> Integer[] (Positions)) */
	private final HashMap<Long, HashMap<String, Integer[]>> positionsByWordById = new HashMap<Long, HashMap<String, Integer[]>>();
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

	private final static String[] BADLIST_EXTENSTION = new String[] { 
		".ico", ".bmp", ".png", ".jpg", ".gif",		// image
		".zip", ".jar", ".gz" , ".bz2", ".rar",		// archive
		".7z" , ".rar", ".arj", ".rpm",	".deb",
		".xpi", ".ace", ".cab", ".lza", ".lzh",
		".ace",
		".exe", ".iso",								// binary
		".mpg", ".ogg", ".mp3", ".avi",				// media
		".css", ".sig"								// other
	};

	/**
	 * Adds the found uri to the list of to-be-retrieved uris. <p>Every usk uri added as ssk.
	 * @param uri the new uri that needs to be fetched for further indexing
	 */
	public void queueURI(FreenetURI uri, String comment) {
		String sURI = uri.toString();
		for (String ext : BADLIST_EXTENSTION)
			if (sURI.endsWith(ext))
				return;	// be smart

		if (uri.isUSK()) {
			if(uri.getSuggestedEdition() < 0)
				uri = uri.setSuggestedEdition((-1)* uri.getSuggestedEdition());
			try{
				uri = ((USK.create(uri)).getSSK()).getURI();
				(ctx.uskManager).subscribe(USK.create(uri),this, false, this);	
			}
			catch(Exception e){}
		}

		synchronized (this) {
			if (getPageByURI(uri) == null) {
				Page page = new Page();
				page.uri = uri.toString();
				page.id = maxPageId.incrementAndGet();
				page.comment = comment;

				db.store(page);
			}
		}
	}

	private void startSomeRequests() {
		FreenetURI[] initialURIs = core.getBookmarkURIs();
		for (int i = 0; i < initialURIs.length; i++)
			queueURI(initialURIs[i], "bookmark");

		ArrayList<ClientGetter> toStart = null;
		synchronized (this) {
			if (stopped) 
				return;
			
			int running = runningFetch.size();
			
			Query query = db.query();
			query.constrain(Page.class);
			query.descend("status").constrain(Status.QUEUED);
			query.descend("lastChange").orderAscending();
			ObjectSet<Page> queuedSet = query.execute();

			if ((running >= maxParallelRequests) || (queuedSet.size() - running <= 0))
				return;

			toStart = new ArrayList<ClientGetter>(maxParallelRequests - running);

			while (running + toStart.size() < maxParallelRequests && queuedSet.hasNext()) {
				Page page = queuedSet.next();

				if (runningFetch.containsKey(page))
					continue;

				try {
					ClientGetter getter = makeGetter(page);

					Logger.minor(this, "Starting " + getter + " " + page);
					toStart.add(getter);
					runningFetch.put(page, getter);
				} catch (MalformedURLException e) {
					Logger.error(this, "IMPOSSIBLE-Malformed URI: " + page, e);

					page.status = Status.FAILED;
					page.lastChange = System.currentTimeMillis();
					db.store(page);
				}
			}
		}
		
		for (ClientGetter g : toStart) {
			try {
				g.start();
				Logger.minor(this, g + " started");
			} catch (FetchException e) {
                Logger.minor(this, "Fetch Exception: " + g, e);
				onFailure(e, g, ((MyClientCallback) g.getClientCallback()).page);
			}
		}
	}


	private class MyClientCallback implements ClientCallback {
		final Page page;
		
		public MyClientCallback(Page page) {
			this.page = page;
		}

		public void onFailure(FetchException e, ClientGetter state) {
			XMLSpider.this.onFailure(e, state, page);
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
			XMLSpider.this.onSuccess(result, state, page);
		}

		public void onSuccess(BaseClientPutter state) {
			// Ignore
		}
		
		public String toString() {
			return super.toString() + ":" + page;
		}		
	}
	
	private ClientGetter makeGetter(Page page) throws MalformedURLException {
		ClientGetter getter = new ClientGetter(new MyClientCallback(page),
		        core.requestStarters.chkFetchScheduler, core.requestStarters.sskFetchScheduler,
		        new FreenetURI(page.uri), ctx, PRIORITY_CLASS, this, null, null);
		return getter;
	}

	/**
	 * Processes the successfully fetched uri for further outlinks.
	 * 
	 * @param result
	 * @param state
	 * @param page
	 */
	public void onSuccess(FetchResult result, ClientGetter state, Page page) {
		synchronized (this) {
			if (stopped)
				return;
		}
		
		FreenetURI uri = state.getURI();
		page.status = Status.SUCCEEDED; // Content filter may throw, but we mark it as success anyway

		try {
			ClientMetadata cm = result.getMetadata();
			Bucket data = result.asBucket();
			String mimeType = cm.getMIMEType();			
			
			/*
			 * instead of passing the current object, the pagecallback object for every page is passed to the content filter
			 * this has many benefits to efficiency, and allows us to identify trivially which page is being indexed.
			 * (we CANNOT rely on the base href provided).
			 */
			PageCallBack pageCallBack = new PageCallBack(page);
			Logger.minor(this, "Successful: " + uri + " : " + page.id);
			
			try {
				ContentFilter.filter(data, new NullBucketFactory(), mimeType, uri.toURI("http://127.0.0.1:8888/"), pageCallBack);
				Logger.minor(this, "Filtered " + uri + " : " + page.id);
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
				runningFetch.remove(page);
				page.lastChange = System.currentTimeMillis();
				db.store(page);
			}
			startSomeRequests();
		}
	}

	public void onFailure(FetchException fe, ClientGetter state, Page page) {
		Logger.minor(this, "Failed: " + page + " : " + state, fe);

		synchronized (this) {
			if (stopped)
				return;

			runningFetch.remove(page);
			
			if (fe.newURI != null) {
				// redirect, mark as succeeded
				queueURI(fe.newURI, "redirect from " + state.getURI());

				page.status = Status.SUCCEEDED;
				page.lastChange = System.currentTimeMillis();
				db.store(page);
			} else if (fe.isFatal()) {
				// too many tries or fatal, mark as failed
				page.status = Status.FAILED;
				page.lastChange = System.currentTimeMillis();
				db.store(page);
			} else {
				// requeue at back
				page.status = Status.QUEUED;
				page.lastChange = System.currentTimeMillis();

				db.store(page);
			}
		}
		
		startSomeRequests();
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
	private synchronized void makeMainIndex() throws IOException,NoSuchAlgorithmException {
		// Produce the main index file.
		Logger.minor(this, "Producing top index...");

		if (idsByWord.isEmpty()) {
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
			subIndexElement.setAttribute("key", indices.elementAt(i));
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
	private synchronized void makeSubIndices() throws Exception{
		Logger.normal(this, "Generating index...");
		//using the tMap generate the xml indices
		if (idsByWord.isEmpty()) {
			System.out.println("No URIs with words");
			return;
		}

		Query query = db.query();
		query.constrain(Term.class);
		query.descend("md5").orderAscending();
		ObjectSet<Term> termSet = query.execute();
		
		indices = new Vector<String>();
		int prefix = (int) ((Math.log(termSet.size()) - Math.log(MAX_ENTRIES)) / Math.log(16)) - 1;
		if (prefix <= 0) prefix = 1;
		match = 1;
		Vector<String> list = new Vector<String>();

		String str = termSet.get(0).md5;
		String currentPrefix = str.substring(0, prefix);
		list.add(str);
				
		int i = 0;
		for (Term term : termSet)
		{
			String key = term.md5;
			//create a list of the words to be added in the same subindex
			if (key.startsWith(currentPrefix)) 
			{i++;
			list.add(key);
			}
			else {
				//generate the appropriate subindex with the current list
				generateSubIndex(prefix,list);
				str = key;
				currentPrefix = str.substring(0, prefix);
				list = new Vector<String>();
				list.add(key);
			}
		}

		generateSubIndex(prefix,list);
	}


	private synchronized void generateSubIndex(int p, List<String> list) throws Exception {
		boolean logMINOR = Logger.shouldLog(Logger.MINOR, this);
		/*
		 * if the list is less than max allowed entries in a file then directly generate the xml 
		 * otherwise split the list into further sublists
		 * and iterate till the number of entries per subindex is less than the allowed value
		 */
		if(logMINOR)
			Logger.minor(this, "Generating subindex for "+list.size()+" entries with prefix length "+p);

		try {
			if (list.size() == 0)
				return;
			if (list.size() < MAX_ENTRIES)
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
			String str = list.get(i);
			int index=0;
			while(i<list.size())
			{
				String key = list.get(i);
				if((key.substring(0, prefix)).equals(str.substring(0, prefix))) 
				{
					i++;
				}
				else {
					generateSubIndex(prefix, list.subList(index, i));
					index = i;
					str = key;
				}
			}
			generateSubIndex(prefix, list.subList(index, i));
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
	protected synchronized void generateXML(List<String> list, int prefix) throws TooBigIndexException, Exception
	{
		String p = list.get(0).substring(0, prefix);
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
		Vector<Long> fileid = new Vector<Long>();
		for(int i =0;i<list.size();i++)
		{
			Element wordElement = xmlDoc.createElement("word");
			String str = getTermByMd5(list.get(i)).word;
			wordElement.setAttribute("v",str );
			Long[] idsForWord = idsByWord.get(str);
			for (int j = 0; j < idsForWord.length; j++) {
				Long id = idsForWord[j];
					Long x = id;
				if (x == null) {
					Logger.error(this, "Eh?");
					continue;
				}
				
				Page page = getPageById(id);
				
				/*
				 * adding file information
				 * uriElement - lists the id of the file containing a particular word
				 * fileElement - lists the id,key,title of the files mentioned in the entire subindex
				 */
				Element uriElement = xmlDoc.createElement("file");
				Element fileElement = xmlDoc.createElement("file");
				uriElement.setAttribute("id", x.toString());
				fileElement.setAttribute("id", x.toString());
				fileElement.setAttribute("key", page.uri);
				fileElement.setAttribute("title", page.pageTitle != null ? page.pageTitle : page.uri);
				
				/* Position by position */

				HashMap<String, Integer[]> positionsForGivenWord = positionsByWordById.get(x);
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
	private static String MD5(String text) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] md5hash = new byte[32];
			byte[] b = text.getBytes("UTF-8");
			md.update(b, 0, b.length);
			md5hash = md.digest();
			return convertToHex(md5hash);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("UTF-8 not supported", e);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("MD5 not supported", e);
		}
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
			for (Map.Entry<Page, ClientGetter> me : runningFetch.entrySet()) {
				me.getValue().cancel();
			}
			runningFetch.clear();
		}
	}

	public void runPlugin(PluginRespirator pr){
		this.pr = pr;
		this.core = pr.getNode().clientCore;

		/* Initialize Fetch Context */
		this.ctx = core.makeClient((short) 0).getFetchContext();
		ctx.maxSplitfileBlockRetries = 2;
		ctx.maxNonSplitfileRetries = 2;
		ctx.maxTempLength = 2 * 1024 * 1024;
		ctx.maxOutputLength = 2 * 1024 * 1024;
		allowedMIMETypes = new HashSet<String>();
		allowedMIMETypes.add("text/html");
		allowedMIMETypes.add("text/plain");
		allowedMIMETypes.add("application/xhtml+xml");
		ctx.allowedMIMETypes = new HashSet<String>(allowedMIMETypes);

		tProducedIndex = System.currentTimeMillis();
		stopped = false;
		
		if (!new File(DEFAULT_INDEX_DIR).mkdirs()) {
			Logger.error(this, "Could not create default index directory ");
		}

		// Initial DB4O
		db = initDB4O();
		
		// Find max Page ID
		{
			Query query = db.query();
			query.constrain(Page.class);
			query.descend("id").orderDescending();
			ObjectSet<Page> set = query.execute();
			if (set.hasNext())
				maxPageId = new AtomicLong(set.next().id);
			else
				maxPageId = new AtomicLong(0);
		}
		
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
			appendList(listname, out);
			return out.toString();
		}
		appendDefaultPageStart(out,null);
		String uriParam = request.getParam("adduri");
		if(uriParam != null && uriParam.length() != 0)
		{
			try {
				FreenetURI uri = new FreenetURI(uriParam);
				synchronized (this) {
					// Check if queued already
					Page page = getPageByURI(uri);
					if (page != null) {
						// We have no reliable way to stop a request,
						// requeue only if it is successed / failed
						if (page.status == Status.SUCCEEDED || page.status == Status.FAILED) {
							page.lastChange = System.currentTimeMillis();
							page.status = Status.QUEUED;

							db.store(page);
						}
					}
				}
				out.append("<p>URI added :"+uriParam+"</p>");
				queueURI(uri, "manually");
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
	private synchronized void appendList(String listname, StringBuilder out) {
		Iterable<Page> it = runningFetch.keySet();

		if (listname.equals("running")) {
			it = runningFetch.keySet();
		} else if (listname.equals("visited")) {
			Query query = db.query();
			query.constrain(Page.class);
			query.descend("status").constrain(Status.SUCCEEDED);
			query.descend("lastChange").orderAscending();
			ObjectSet<Page> set = query.execute();

			it = set;
		} else if (listname.equals("queued")) {
			Query query = db.query();
			query.constrain(Page.class);
			query.descend("status").constrain(Status.QUEUED);
			query.descend("lastChange").orderAscending();
			ObjectSet<Page> set = query.execute();

			it = set;
		} else if (listname.equals("failed")) {
			Query query = db.query();
			query.constrain(Page.class);
			query.descend("status").constrain(Status.FAILED);
			query.descend("lastChange").orderAscending();
			ObjectSet<Page> set = query.execute();

			it = set;
		}
		
		for (Page page : it)
			out.append("<code title=\"" + HTMLEncoder.encode(page.comment) + "\">" + HTMLEncoder.encode(page.uri)
			        + "</code><br/>");
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
		List<Page> runningFetchesSnapshot;
		long runningFetchesSnapshotSize;
		List<Page> visitedSnapshot;
		long visitedSnapshotSize;
		List<Page> failedSnapshot;
		long failedSnapshotSize;
		List<Page> queuedSnapshot;
		long queuedSnapshotSize;
		
		synchronized(this) {
			runningFetchesSnapshot = new ArrayList<Page>(maxShownURIs);
			{
				Iterator<Page> it = this.runningFetch.keySet().iterator();
				for (int i = 0; it.hasNext() && i < maxShownURIs; i++)
					runningFetchesSnapshot.add(it.next());
				runningFetchesSnapshotSize = runningFetch.size();
			}

			visitedSnapshot = new ArrayList<Page>(maxShownURIs);
			Query query = db.query();
			query.constrain(Page.class);
			query.descend("status").constrain(Status.SUCCEEDED);
			query.descend("lastChange").orderAscending();
			ObjectSet<Page> set = query.execute();
			for (int i = 0; set.hasNext() && i < maxShownURIs; i++)
				visitedSnapshot.add(set.next());
			visitedSnapshotSize = set.size();

			failedSnapshot = new ArrayList<Page>(maxShownURIs);
			query = db.query();
			query.constrain(Page.class);
			query.descend("status").constrain(Status.FAILED);
			query.descend("lastChange").orderAscending();
			set = query.execute();
			for (int i = 0; set.hasNext() && i < maxShownURIs; i++)
				failedSnapshot.add(set.next());
			failedSnapshotSize = set.size();

			queuedSnapshot = new ArrayList<Page>(maxShownURIs);
			query = db.query();
			query.constrain(Page.class);
			query.descend("status").constrain(Status.QUEUED);
			query.descend("lastChange").orderAscending();
			set = query.execute();
			for (int i = 0; set.hasNext() && i < maxShownURIs; i++)
				queuedSnapshot.add(set.next());
			queuedSnapshotSize = set.size();
		}
		
		out.append("<p><h3>Running Fetches</h3></p>");
		out.append("<br/>Size :" + runningFetchesSnapshotSize + "<br/>");
		for (Page page : runningFetchesSnapshot)
			out.append("<code title=\"" + HTMLEncoder.encode(page.comment) + "\">" + HTMLEncoder.encode(page.uri)
			        + "</code><br/>");
		out.append("<p><a href=\"?list="+"running"+"\">Show all</a><br/></p>");

		
		out.append("<p><h3>Queued URIs</h3></p>");
		out.append("<br/>Size :" + queuedSnapshotSize + "<br/>");
		for (Page page : queuedSnapshot)
			out.append("<code title=\"" + HTMLEncoder.encode(page.comment) + "\">" + HTMLEncoder.encode(page.uri)
			        + "</code><br/>");
		out.append("<p><a href=\"?list=\">Show all</a><br/></p>");
	
	
		out.append("<p><h3>Visited URIs</h3></p>");
		out.append("<br/>Size :" + visitedSnapshotSize + "<br/>");
		for (Page page : visitedSnapshot)
			out.append("<code title=\"" + HTMLEncoder.encode(page.comment) + "\">" + HTMLEncoder.encode(page.uri)
			        + "</code><br/>");
		out.append("<p><a href=\"?list="+"visited"+"\">Show all</a><br/></p>");
		
		out.append("<p><h3>Failed URIs</h3></p>");
		out.append("<br/>Size :" + failedSnapshotSize + "<br/>");
		for (Page page : failedSnapshot)
			out.append("<code title=\"" + HTMLEncoder.encode(page.comment) + "\">" + HTMLEncoder.encode(page.uri)
			        + "</code><br/>");
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

	/**
	 * creates the callback object for each page.
	 *<p>Used to create inlinks and outlinks for each page separately.
	 * @author swati
	 *
	 */
	public class PageCallBack implements FoundURICallback{
		final Page page;

		PageCallBack(Page page) {
			this.page = page;
		}

		public void foundURI(FreenetURI uri){
			// Ignore
		}
		
		public void foundURI(FreenetURI uri, boolean inline){
			Logger.debug(this, "foundURI " + uri + " on " + page);
			queueURI(uri, "Added from " + page.uri);
		}

		Integer lastPosition = null;
		
		public void onText(String s, String type, URI baseURI){
			Logger.debug(this, "onText on " + page.id + " (" + baseURI + ")");

			if ("title".equalsIgnoreCase(type) && (s != null) && (s.length() != 0) && (s.indexOf('\n') < 0)) {
				/*
				 * title of the page 
				 */
				page.pageTitle = s;
				type = "title";
			}
			else type = null;
			/*
			 * determine the position of the word in the retrieved page
			 * FIXME - replace with a real tokenizor
			 */
			String[] words = s.split("[^\\p{L}\\{N}]");

			if(lastPosition == null)
				lastPosition = 1; 
			for (int i = 0; i < words.length; i++) {
				String word = words[i];
				if ((word == null) || (word.length() == 0))
					continue;
				word = word.toLowerCase();
				word = word.intern();
				try{
					if(type == null)
						addWord(word, lastPosition.intValue() + i, page.id);
					else
						addWord(word, -1 * (i + 1), page.id);
				}
				catch (Exception e){}
			}

			if(type == null) {
				lastPosition = lastPosition + words.length;
			}
		}

		private void addWord(String word, int position, Long id) throws Exception {
			synchronized(XMLSpider.this) {
				if (word.length() < 3)
					return;

				Long[] ids = idsByWord.get(word);

				/* Word position indexation */
				HashMap<String, Integer[]> wordPositionsForOneUri = positionsByWordById.get(id);
				/* For a given URI , take as key a word , and gives position */
				if (wordPositionsForOneUri == null) {
					wordPositionsForOneUri = new HashMap<String, Integer[]>();
					wordPositionsForOneUri.put(word, new Integer[] { position });
					positionsByWordById.put(id, wordPositionsForOneUri);
				} 
				else {
					Integer[] positions = wordPositionsForOneUri.get(word);
					if (positions == null) {
						positions = new Integer[] { position };
						wordPositionsForOneUri.put(word, positions);
					} else {
						Integer[] newPositions = new Integer[positions.length + 1];
						System.arraycopy(positions, 0, newPositions, 0, positions.length);
						newPositions[positions.length] = position;
						wordPositionsForOneUri.put(word, newPositions);
					}
				}

				if (ids == null) {
					idsByWord.put(word, new Long[] { id });
				} else {
					for (int i = 0; i < ids.length; i++) {
						if (ids[i].equals(id))
							return;
					}
					Long[] newIDs = new Long[ids.length + 1];
					System.arraycopy(ids, 0, newIDs, 0, ids.length);
					newIDs[ids.length] = id;
					idsByWord.put(word, newIDs);
				}

				synchronized (db) {
					if (getTermByWord(word) == null)
						db.store(new Term(word));
				}
				//long time_indexing = System.currentTimeMillis();
				//			FileWriter outp = new FileWriter("logfile",true);
				mustWriteIndex = true;
			}
		}
	}

	private boolean mustWriteIndex = false;
	private boolean writingIndex;
	
	public void makeIndex() throws Exception {
		synchronized(this) {
			if (writingIndex || stopped)
				return;

			db.commit();
			writingIndex = true;
		}

		try {
			synchronized(this) {
				if(!mustWriteIndex) {
					Logger.minor(this, "Not making index, no data added since last time");
					return;
				}
				mustWriteIndex = false;
			}
			time_taken = System.currentTimeMillis();

			makeSubIndices();
			makeMainIndex();

			time_taken = System.currentTimeMillis() - time_taken;
			tProducedIndex = System.currentTimeMillis();

			Logger.minor(this, "Made index, took " + time_taken);
		} finally {
			if (!stopped)
				scheduleMakeIndex();
			writingIndex = false;
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
		/*-
		 * FIXME this code don't make sense 
		 *  (1) runningFetchesByURI contain SSK, not USK
		 *  (2) onFoundEdition always have the edition set
		 *  
		if(runningFetchesByURI.containsKey(uri)) runningFetchesByURI.remove(uri);
		uri = key.getURI().setSuggestedEdition(l);
		 */
		queueURI(uri, "USK found edition");
	}

	public short getPollingPriorityNormal() {
		return (short) Math.min(RequestStarter.MINIMUM_PRIORITY_CLASS, PRIORITY_CLASS + 1);
	}

	public short getPollingPriorityProgress() {
		return PRIORITY_CLASS;
	}
	
	/**
	 * Initializes DB4O.
	 * 
	 * @return db4o's connector
	 */
	protected ObjectContainer db;

	private ObjectContainer initDB4O() {
		Configuration cfg = Db4o.newConfiguration();

		//- Page
		cfg.objectClass(Page.class).objectField("id").indexed(true);
		cfg.objectClass(Page.class).objectField("uri").indexed(true);
		cfg.objectClass(Page.class).objectField("status").indexed(true);
		cfg.objectClass(Page.class).objectField("lastChange").indexed(true);		

		cfg.objectClass(Page.class).callConstructor(true);

		cfg.objectClass(Page.class).cascadeOnActivate(true);
		cfg.objectClass(Page.class).cascadeOnUpdate(true);
		cfg.objectClass(Page.class).cascadeOnDelete(true);
		
		//- Term
		cfg.objectClass(Term.class).objectField("md5").indexed(true);
		cfg.objectClass(Term.class).objectField("word").indexed(true);

		cfg.objectClass(Term.class).callConstructor(true);

		cfg.objectClass(Term.class).cascadeOnActivate(true);
		cfg.objectClass(Term.class).cascadeOnUpdate(true);
		cfg.objectClass(Term.class).cascadeOnDelete(true);

		//- Other
		cfg.activationDepth(1);
		cfg.updateDepth(1);
		cfg.queries().evaluationMode(QueryEvaluationMode.LAZY);
		cfg.diagnostic().addListener(new DiagnosticToConsole());

		File db = new File("XMLSpider-" + version + ".db4o");
		db.delete();

		ObjectContainer oc = Db4o.openFile(cfg, "XMLSpider-" + version + ".db4o");
		db.deleteOnExit();

		return oc;
	}
	
	protected Page getPageByURI(FreenetURI uri) {
		Query query = db.query();
		query.constrain(Page.class);
		query.descend("uri").constrain(uri.toString());
		ObjectSet<Page> set = query.execute();

		if (set.hasNext())
			return set.next();
		else
			return null;
	}

	protected Page getPageById(long id) {
		Query query = db.query();
		query.constrain(Page.class);
		query.descend("id").constrain(id);
		ObjectSet<Page> set = query.execute();

		if (set.hasNext())
			return set.next();
		else
			return null;
	}
	
	protected Term getTermByMd5(String md5) {
		Query query = db.query();
		query.constrain(Term.class);
		query.descend("md5").constrain(md5);
		ObjectSet<Term> set = query.execute();

		if (set.hasNext())
			return set.next();
		else
			return null;
	}

	protected Term getTermByWord(String word) {
		Query query = db.query();
		query.constrain(Term.class);
		query.descend("word").constrain(word);
		ObjectSet<Term> set = query.execute();

		if (set.hasNext())
			return set.next();
		else
			return null;
	}
}
