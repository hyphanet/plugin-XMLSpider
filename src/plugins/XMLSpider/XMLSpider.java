/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package plugins.XMLSpider;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.db4o.Db4o;
import com.db4o.ObjectContainer;
import com.db4o.ObjectSet;
import com.db4o.config.Configuration;
import com.db4o.config.QueryEvaluationMode;
import com.db4o.diagnostic.DiagnosticToConsole;
import com.db4o.query.Query;
import com.db4o.reflect.jdk.JdkReflector;

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
import freenet.clients.http.filter.ContentFilter;
import freenet.clients.http.filter.FoundURICallback;
import freenet.clients.http.filter.UnsafeContentTypeException;
import freenet.keys.FreenetURI;
import freenet.keys.USK;
import freenet.l10n.L10n.LANGUAGE;
import freenet.node.NodeClientCore;
import freenet.node.RequestStarter;
import freenet.pluginmanager.FredPlugin;
import freenet.pluginmanager.FredPluginHTTP;
import freenet.pluginmanager.FredPluginL10n;
import freenet.pluginmanager.FredPluginThreadless;
import freenet.pluginmanager.FredPluginVersioned;
import freenet.pluginmanager.PluginHTTPException;
import freenet.pluginmanager.PluginRespirator;
import freenet.support.HTMLNode;
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
public class XMLSpider implements FredPlugin, FredPluginHTTP, FredPluginThreadless, FredPluginVersioned, FredPluginL10n, USKCallback {
	public synchronized long getNextPageId() {
		long x = maxPageId.incrementAndGet();
		db.store(maxPageId);
		return x;
	}

	/** Document ID of fetching documents */
	protected Map<Page, ClientGetter> runningFetch = Collections.synchronizedMap(new HashMap<Page, ClientGetter>());

	protected MaxPageId maxPageId;
	
	/**
	 * directory where the generated indices are stored. 
	 * Needs to be created before it can be used
	 */
	public static final String DEFAULT_INDEX_DIR = "myindex7/";
	/**
	 * Lists the allowed mime types of the fetched page. 
	 */
	public Set<String> allowedMIMETypes;
	static final int MAX_ENTRIES = 800;
	static final long MAX_SUBINDEX_UNCOMPRESSED_SIZE = 4 * 1024 * 1024;
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

	static final String indexTitle = "XMLSpider index";
	static final String indexOwner = "Freenet";
	static final String indexOwnerEmail = null;

	// Can have many; this limit only exists to save memory.
	private static final int maxParallelRequests = 100;
	private int maxShownURIs = 15;

	private NodeClientCore core;
	private FetchContext ctx;
	// Equal to Frost, ARK fetches etc. One step down from Fproxy.
	// Any lower makes it very difficult to debug. Maybe reduce for production - after solving the ARK bugs.
	private final short PRIORITY_CLASS = RequestStarter.IMMEDIATE_SPLITFILE_PRIORITY_CLASS;
	private boolean stopped = true;

	private PageMaker pageMaker;

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
	public void queueURI(FreenetURI uri, String comment, boolean force) {
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
			Page page = getPageByURI(uri);
			if (page == null) {
				page = new Page(getNextPageId(), uri.toString(), comment);

				db.store(page);
			} else if (force) {
				synchronized (page) {
					page.status = Status.QUEUED;
					page.lastChange = System.currentTimeMillis();

					db.store(page);
				}
			}
		}
	}

	protected List<Page> queuedRequestCache = new ArrayList<Page>();

	private void startSomeRequests() {
		FreenetURI[] initialURIs = core.getBookmarkURIs();
		for (int i = 0; i < initialURIs.length; i++)
			queueURI(initialURIs[i], "bookmark", false);

		ArrayList<ClientGetter> toStart = null;
		synchronized (this) {
			if (stopped)
				return;
			synchronized (runningFetch) {
				int running = runningFetch.size();

				if (running >= maxParallelRequests) return;

				if (queuedRequestCache.isEmpty()) {
					Query query = db.query();
					query.constrain(Page.class);
					query.descend("status").constrain(Status.QUEUED);
					query.descend("lastChange").orderAscending();
					@SuppressWarnings("unchecked")
					ObjectSet<Page> queuedSet = query.execute();

					for (int i = 0 ; 
						i < maxParallelRequests * 2 && queuedSet.hasNext();
						i++) {	// cache 2 * maxParallelRequests
						queuedRequestCache.add(queuedSet.next());
					}
				}
				queuedRequestCache.removeAll(runningFetch.keySet());

				toStart = new ArrayList<ClientGetter>(maxParallelRequests - running);

				Iterator<Page> it = queuedRequestCache.iterator();

				while (running + toStart.size() < maxParallelRequests && it.hasNext()) {
					Page page = it.next();
					it.remove();

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
		}

		for (ClientGetter g : toStart) {
			try {
				g.start();
				Logger.minor(this, g + " started");
			} catch (FetchException e) {
				Logger.minor(this, "Fetch Exception: " + g, e);
				g.getClientCallback().onFailure(e, g);
			}
		}
	}

	private class MyClientCallback implements ClientCallback {
		final Page page;
		Status status; // for debug

		public MyClientCallback(Page page) {
			this.page = page;
			this.status = Status.QUEUED;
		}

		public void onFailure(FetchException e, ClientGetter state) {
			if (!stopped)
				callbackExecutor.execute(new OnFailureCallback(e, state, page));
			status = Status.FAILED;
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

		public void onSuccess(final FetchResult result, final ClientGetter state) {
			if (!stopped)
				callbackExecutor.execute(new OnSuccessCallback(result, state, page));
			status = Status.SUCCEEDED;
		}

		public void onSuccess(BaseClientPutter state) {
			// Ignore
		}

		public String toString() {
			return super.toString() + ":" + page + "(" + status + ")";
		}		
	}

	private ClientGetter makeGetter(Page page) throws MalformedURLException {
		ClientGetter getter = new ClientGetter(new MyClientCallback(page),
				core.requestStarters.chkFetchScheduler,
		        core.requestStarters.sskFetchScheduler, new FreenetURI(page.uri), ctx, PRIORITY_CLASS, this, null, null);
		return getter;
	}

	protected class OnFailureCallback implements Runnable {
		private FetchException e;
		private ClientGetter state;
		private Page page;

		OnFailureCallback(FetchException e, ClientGetter state, Page page) {
			this.e = e;
			this.state = state;
			this.page = page;
		}

		public void run() {
			onFailure(e, state, page);
		}
	}

	protected class OnSuccessCallback implements Runnable {
		private FetchResult result;
		private ClientGetter state;
		private Page page;

		OnSuccessCallback(FetchResult result, ClientGetter state, Page page) {
			this.result = result;
			this.state = state;
			this.page = page;
		}

		public void run() {
			onSuccess(result, state, page);
		}
	}

	private synchronized void scheduleMakeIndex() {
		if (writeIndexScheduled || writingIndex)
			return;

		callbackExecutor.execute(new MakeIndexCallback());
		writeIndexScheduled = true;
	}
	
	protected class MakeIndexCallback implements Runnable {
		public void run() {
			try {
				synchronized (this) {
					writingIndex = true;
				}
				
				indexWriter.makeIndex();

				synchronized (this) {
					writingIndex = false;
					writeIndexScheduled = false;
				}				
			} catch (Exception e) {
				Logger.error(this, "Could not generate index: "+e, e);
			} finally {
				synchronized (this) {
					writingIndex = false;
					notifyAll();
				}
			}
		}
	}

	protected static class CallbackPrioritizer implements Comparator<Runnable> {
		public int compare(Runnable o1, Runnable o2) {
			if (o1.getClass() == o2.getClass())
				return 0;
			
			return getPriority(o1) - getPriority(o2);
		}

		private int getPriority(Runnable r) {
			if (r instanceof MakeIndexCallback)
				return 0;
			else if (r instanceof OnFailureCallback)
				return 1;
			else if (r instanceof OnSuccessCallback)
				return 2;

			return -1;
		}
	}

	// this is java.util.concurrent.Executor, not freenet.support.Executor
	// always run with one thread --> more thread cause contention and slower!
	protected ThreadPoolExecutor callbackExecutor = new ThreadPoolExecutor( //
	        1, 1, 600, TimeUnit.SECONDS, //
	        new PriorityBlockingQueue<Runnable>(5, new CallbackPrioritizer()), //
	        new ThreadFactory() {
		        public Thread newThread(Runnable r) {
			        Thread t = new NativeThread(r, "XMLSpider", NativeThread.NORM_PRIORITY - 1, true);
			        t.setDaemon(true);
			        return t;
		        }
	        });
	
	/**
	 * Processes the successfully fetched uri for further outlinks.
	 * 
	 * @param result
	 * @param state
	 * @param page
	 */
	// single threaded
	protected void onSuccess(FetchResult result, ClientGetter state, Page page) {
		synchronized (this) {
			if (stopped)
				return;    				
		}

		FreenetURI uri = state.getURI();
		page.status = Status.SUCCEEDED; // Content filter may throw, but we mark it as success anyway

		try {
				// Page may be refetched if added manually
				// Delete existing TermPosition
				Query query = db.query();
				query.constrain(TermPosition.class);
				query.descend("pageId").constrain(page.id);
				@SuppressWarnings("unchecked")
				ObjectSet<TermPosition> set = query.execute();
				for (TermPosition tp : set)
					db.delete(tp);

				ClientMetadata cm = result.getMetadata();
				Bucket data = result.asBucket();
				String mimeType = cm.getMIMEType();

				/*
				 * instead of passing the current object, the pagecallback object for every page is
				 * passed to the content filter this has many benefits to efficiency, and allows us
				 * to identify trivially which page is being indexed. (we CANNOT rely on the base
				 * href provided).
				 */
				PageCallBack pageCallBack = new PageCallBack(page);
				Logger.minor(this, "Successful: " + uri + " : " + page.id);

				try {
					ContentFilter.filter(data, new NullBucketFactory(), mimeType, uri.toURI("http://127.0.0.1:8888/"),
					        pageCallBack);
					pageCallBack.store();
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
			db.commit();
			startSomeRequests();
		}
	}

	protected void onFailure(FetchException fe, ClientGetter state, Page page) {
		Logger.minor(this, "Failed: " + page + " : " + state, fe);

		synchronized (this) {
			if (stopped)
				return;

			synchronized (page) {
				if (fe.newURI != null) {
					// redirect, mark as succeeded
					queueURI(fe.newURI, "redirect from " + state.getURI(), false);

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
			db.commit();
			runningFetch.remove(page);
		}

		startSomeRequests();
	} 

	private boolean writingIndex;
	private boolean writeIndexScheduled;

	protected IndexWriter indexWriter;
	
	public void terminate(){
		synchronized (this) {
			stopped = true;
			
			for (Map.Entry<Page, ClientGetter> me : runningFetch.entrySet()) {
				ClientGetter getter = me.getValue();
				Logger.minor(this, "Canceling request" + getter);
				getter.cancel();
			}
			runningFetch.clear();
			callbackExecutor.shutdownNow();
		}
		try { callbackExecutor.awaitTermination(15, TimeUnit.SECONDS); } catch (InterruptedException e) {}
		try { db.close(); } catch (Exception e) {}
	}

	public synchronized void runPlugin(PluginRespirator pr) {
		this.core = pr.getNode().clientCore;

		this.pageMaker = pr.getPageMaker();
		pageMaker.addNavigationLink("/plugins/plugins.XMLSpider.XMLSpider", "Home", "Home page", false, null);
		pageMaker.addNavigationLink("/plugins/", "Plugins page", "Back to Plugins page", false, null);

		/* Initialize Fetch Context */
		this.ctx = pr.getHLSimpleClient().getFetchContext();
		ctx.maxSplitfileBlockRetries = 2;
		ctx.maxNonSplitfileRetries = 2;
		ctx.maxTempLength = 2 * 1024 * 1024;
		ctx.maxOutputLength = 2 * 1024 * 1024;
		allowedMIMETypes = new HashSet<String>();
		allowedMIMETypes.add("text/html");
		allowedMIMETypes.add("text/plain");
		allowedMIMETypes.add("application/xhtml+xml");
		ctx.allowedMIMETypes = new HashSet<String>(allowedMIMETypes);

		stopped = false;

		if (!new File(DEFAULT_INDEX_DIR).mkdirs()) {
			Logger.error(this, "Could not create default index directory ");
		}

		// Initial DB4O
		db = initDB4O();

		// Find max Page ID
		{
			Query query = db.query();
			query.constrain(MaxPageId.class);
			@SuppressWarnings("unchecked")
			ObjectSet<MaxPageId> set = query.execute();
			
			if (set.hasNext())
				maxPageId = set.next();
			else {
				query = db.query();
				query.constrain(Page.class);
				query.descend("id").orderDescending();
				@SuppressWarnings("unchecked")
				ObjectSet<Page> set2 = query.execute();
				if (set2.hasNext())
					maxPageId = new MaxPageId(set2.next().id);
				else
					maxPageId = new MaxPageId(0);
			}
		}
		
		indexWriter = new IndexWriter(this);

		pr.getNode().executor.execute(new Runnable() {
			public void run() {
				try{
					Thread.sleep(30 * 1000); // Let the node start up
				} catch (InterruptedException e){}
				startSomeRequests();
			}
		}, "Spider Plugin Starter");
	}

	static class PageStatus {
		long count;
		List<Page> pages;

		PageStatus(long count, List<Page> pages) {
			this.count = count;
			this.pages = pages;
		}
	}

	private PageStatus getPageStatus(Status status) {
		Query query = db.query();
		query.constrain(Page.class);
		query.descend("status").constrain(status);
		query.descend("lastChange").orderDescending();
		
		@SuppressWarnings("unchecked")
		ObjectSet<Page> set = query.execute();
		List<Page> pages = new ArrayList<Page>();
		while (set.hasNext() && pages.size() < maxShownURIs) {
			pages.add(set.next());
		}

		return new PageStatus(set.size(), pages);
	}

	private void listPages(PageStatus pageStatus, HTMLNode parent) {
		if (pageStatus.pages.isEmpty()) {
			HTMLNode list = parent.addChild("#", "NO URI");
		} else {
			HTMLNode list = parent.addChild("ol", "style", "overflow: auto; white-space: nowrap;");

			for (Page page : pageStatus.pages) {
				HTMLNode litem = list.addChild("li", "title", page.comment);
				litem.addChild("a", "href", "/freenet:" + page.uri, page.uri);
			}
		}
	}

	/**
	 * Interface to the Spider data
	 */
	public String handleHTTPGet(HTTPRequest request) throws PluginHTTPException {
		HTMLNode pageNode = pageMaker.getPageNode(pluginName, null);
		HTMLNode contentNode = pageMaker.getContentNode(pageNode);

		return generateHTML(request, pageNode, contentNode);
	}

	public String handleHTTPPost(HTTPRequest request) throws PluginHTTPException {
		HTMLNode pageNode = pageMaker.getPageNode(pluginName, null);
		HTMLNode contentNode = pageMaker.getContentNode(pageNode);

		if (request.isPartSet("createIndex")) {
			synchronized (this) {
				scheduleMakeIndex();

				HTMLNode infobox = pageMaker.getInfobox("infobox infobox-success", "Scheduled Creating Index");
				infobox.addChild("#", "Index will start create soon.");
				contentNode.addChild(infobox);
			}
		}

		String addURI = request.getPartAsString("addURI", 512);
		if (addURI != null && addURI.length() != 0) {
			try {
				FreenetURI uri = new FreenetURI(addURI);
				queueURI(uri, "manually", true);

				HTMLNode infobox = pageMaker.getInfobox("infobox infobox-success", "URI Added");
				infobox.addChild("#", "Added " + uri);
				contentNode.addChild(infobox);
			} catch (Exception e) {
				HTMLNode infobox = pageMaker.getInfobox("infobox infobox-error", "Error adding URI");
				infobox.addChild("#", e.getMessage());
				contentNode.addChild(infobox);
				Logger.normal(this, "Manual added URI cause exception", e);
			}

			startSomeRequests();
		}

		return generateHTML(request, pageNode, contentNode);
	}

	private String generateHTML(HTTPRequest request, HTMLNode pageNode, HTMLNode contentNode) {
		HTMLNode overviewTable = contentNode.addChild("table", "class", "column");
		HTMLNode overviewTableRow = overviewTable.addChild("tr");

		PageStatus queuedStatus = getPageStatus(Status.QUEUED);
		PageStatus succeededStatus = getPageStatus(Status.SUCCEEDED);
		PageStatus failedStatus = getPageStatus(Status.FAILED);

		// Column 1
		HTMLNode nextTableCell = overviewTableRow.addChild("td", "class", "first");
		HTMLNode statusBox = pageMaker.getInfobox("Spider Status");
		HTMLNode statusContent = pageMaker.getContentNode(statusBox);
		statusContent.addChild("#", "Running Request: " + runningFetch.size() + "/" + maxParallelRequests);
		statusContent.addChild("br");
		statusContent.addChild("#", "Queued: " + queuedStatus.count);
		statusContent.addChild("br");
		statusContent.addChild("#", "Succeeded: " + succeededStatus.count);
		statusContent.addChild("br");
		statusContent.addChild("#", "Failed: " + failedStatus.count);
		statusContent.addChild("br");
		statusContent.addChild("br");
		statusContent.addChild("#", "Queued Event: " + callbackExecutor.getQueue().size());
		statusContent.addChild("br");
		statusContent.addChild("#", "Index Writer: ");
		synchronized (this) {
			if (writingIndex)
				statusContent.addChild("span", "style", "color: red; font-weight: bold;", "RUNNING");
			else if (writeIndexScheduled)
				statusContent.addChild("span", "style", "color: blue; font-weight: bold;", "SCHEDULED");
			else
				statusContent.addChild("span", "style", "color: green; font-weight: bold;", "IDLE");
		}
		statusContent.addChild("br");
		statusContent.addChild("#", "Last Written: "
				+ (indexWriter.tProducedIndex == 0 ? "NEVER" : new Date(indexWriter.tProducedIndex).toString()));
		nextTableCell.addChild(statusBox);

		// Column 2
		nextTableCell = overviewTableRow.addChild("td", "class", "second");
		HTMLNode mainBox = pageMaker.getInfobox("Main");
		HTMLNode mainContent = pageMaker.getContentNode(mainBox);
		HTMLNode addForm = mainContent.addChild("form", //
				new String[] { "action", "method" }, //
		        new String[] { "plugins.XMLSpider.XMLSpider", "post" });
		addForm.addChild("label", "for", "addURI", "Add URI:");
		addForm.addChild("input", new String[] { "name", "style" }, new String[] { "addURI", "width: 20em;" });
		addForm.addChild("input", //
				new String[] { "name", "type", "value" },//
		        new String[] { "formPassword", "hidden", core.formPassword });
		addForm.addChild("input", "type", "submit");
		nextTableCell.addChild(mainBox);

		HTMLNode indexBox = pageMaker.getInfobox("Create Index");
		HTMLNode indexContent = pageMaker.getContentNode(indexBox);
		HTMLNode indexForm = indexContent.addChild("form", //
				new String[] { "action", "method" }, //
		        new String[] { "plugins.XMLSpider.XMLSpider", "post" });
		indexForm.addChild("input", //
				new String[] { "name", "type", "value" },//
		        new String[] { "formPassword", "hidden", core.formPassword });
		indexForm.addChild("input", //
				new String[] { "name", "type", "value" },//
		        new String[] { "createIndex", "hidden", "createIndex" });
		indexForm.addChild("input", //
				new String[] { "type", "value" }, //
		        new String[] { "submit", "Create Index Now" });
		nextTableCell.addChild(indexBox);

		HTMLNode runningBox = pageMaker.getInfobox("Running URI");
		runningBox.addAttribute("style", "right: 0;");
		HTMLNode runningContent = pageMaker.getContentNode(runningBox);
		synchronized (runningFetch) {
			if (runningFetch.isEmpty()) {
				HTMLNode list = runningContent.addChild("#", "NO URI");
			} else {
				HTMLNode list = runningContent.addChild("ol", "style", "overflow: auto; white-space: nowrap;");

				Iterator<Page> pi = runningFetch.keySet().iterator();
				for (int i = 0; i < maxShownURIs && pi.hasNext(); i++) {
					Page page = pi.next();
					HTMLNode litem = list.addChild("li", "title", page.comment);
					litem.addChild("a", "href", "/freenet:" + page.uri, page.uri);
				}
			}
		}
		contentNode.addChild(runningBox);

		HTMLNode queuedBox = pageMaker.getInfobox("Queued URI");
		queuedBox.addAttribute("style", "right: 0; overflow: auto;");
		HTMLNode queuedContent = pageMaker.getContentNode(queuedBox);
		listPages(queuedStatus, queuedContent);
		contentNode.addChild(queuedBox);

		HTMLNode succeededBox = pageMaker.getInfobox("Succeeded URI");
		succeededBox.addAttribute("style", "right: 0;");
		HTMLNode succeededContent = pageMaker.getContentNode(succeededBox);
		listPages(succeededStatus, succeededContent);
		contentNode.addChild(succeededBox);

		HTMLNode failedBox = pageMaker.getInfobox("Failed URI");
		failedBox.addAttribute("style", "right: 0;");
		HTMLNode failedContent = pageMaker.getContentNode(failedBox);
		listPages(failedStatus, failedContent);
		contentNode.addChild(failedBox);

		return pageNode.generate();
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
			queueURI(uri, "Added from " + page.uri, false);
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
			String[] words = s.split("[^\\p{L}\\{N}]+");

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
						addWord(word, lastPosition.intValue() + i);
					else
						addWord(word, -1 * (i + 1));
				}
				catch (Exception e){}
			}

			if(type == null) {
				lastPosition = lastPosition + words.length;
			}
		}

		private void addWord(String word, int position) throws Exception {
			if (word.length() < 3)
				return;
			Term term = getTermByWord(word, true);
			TermPosition termPos = getTermPosition(term, true);

			synchronized (termPos) {
				int[] newPositions = new int[termPos.positions.length + 1];
				System.arraycopy(termPos.positions, 0, newPositions, 0, termPos.positions.length);
				newPositions[termPos.positions.length] = position;

				termPos.positions = newPositions;
			}
		}
		
		@SuppressWarnings("serial")
		protected Map<Term, TermPosition> termPosCache = new LinkedHashMap<Term, TermPosition>() {
			protected boolean removeEldestEntry(Map.Entry<Term, TermPosition> eldest) {
				if (size() < 1024) return false;
				
				db.store(eldest.getValue());
				return true;
			}
		};

		public void store() {
			for (TermPosition tp : termPosCache.values())
				db.store(tp);
			termPosCache.clear();
		}

		protected TermPosition getTermPosition(Term term, boolean create) {
			synchronized (term) {
				TermPosition cachedTermPos = termPosCache.get(term);
				if (cachedTermPos != null)
					return cachedTermPos;

				synchronized (page) {
					Query query = db.query();
					query.constrain(TermPosition.class);

					query.descend("word").constrain(term.word);
					query.descend("pageId").constrain(page.id);
					@SuppressWarnings("unchecked")
					ObjectSet<TermPosition> set = query.execute();

					if (set.hasNext()) {
						cachedTermPos = set.next();
						termPosCache.put(term, cachedTermPos);
						return cachedTermPos;
					} else if (create) {
						cachedTermPos = new TermPosition();
						cachedTermPos.word = term.word;
						cachedTermPos.pageId = page.id;
						cachedTermPos.positions = new int[0];

						termPosCache.put(term, cachedTermPos);
						db.store(cachedTermPos);
						return cachedTermPos;
					} else {
						return null;
					}
				}
			}
		}
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
		queueURI(uri, "USK found edition", true);
		startSomeRequests();
	}

	public short getPollingPriorityNormal() {
		return (short) Math.min(RequestStarter.MINIMUM_PRIORITY_CLASS, PRIORITY_CLASS + 1);
	}

	public short getPollingPriorityProgress() {
		return PRIORITY_CLASS;
	}

	protected ObjectContainer db;

	/**
	 * Initializes DB4O.
	 * 
	 * @return db4o's connector
	 */
	private ObjectContainer initDB4O() {
		Configuration cfg = Db4o.newConfiguration();
		cfg.reflectWith(new JdkReflector(getClass().getClassLoader()));

		//- Page
		cfg.objectClass(Page.class).objectField("id").indexed(true);
		cfg.objectClass(Page.class).objectField("uri").indexed(true);
		cfg.objectClass(Page.class).objectField("status").indexed(true);
		cfg.objectClass(Page.class).objectField("lastChange").indexed(true);		

		cfg.objectClass(Page.class).callConstructor(true);

		//- Term
		cfg.objectClass(Term.class).objectField("md5").indexed(true);
		cfg.objectClass(Term.class).objectField("word").indexed(true);

		cfg.objectClass(Term.class).callConstructor(true);

		//- TermPosition
		cfg.objectClass(TermPosition.class).objectField("pageId").indexed(true);
		cfg.objectClass(TermPosition.class).objectField("word").indexed(true);

		cfg.objectClass(TermPosition.class).callConstructor(true);

		//- Other
		cfg.activationDepth(1);
		cfg.updateDepth(1);
		// cfg.automaticShutDown(false);
		cfg.queries().evaluationMode(QueryEvaluationMode.LAZY);
		cfg.diagnostic().addListener(new DiagnosticToConsole());

		ObjectContainer oc = Db4o.openFile(cfg, "XMLSpider-" + version + ".db4o");

		return oc;
	}

	protected Page getPageByURI(FreenetURI uri) {
		Query query = db.query();
		query.constrain(Page.class);
		query.descend("uri").constrain(uri.toString());
		@SuppressWarnings("unchecked")
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
		@SuppressWarnings("unchecked")
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
		@SuppressWarnings("unchecked")
		ObjectSet<Term> set = query.execute();

		if (set.hasNext())
			return set.next();
		else
			return null;
	}

	@SuppressWarnings("serial")	
	protected Map<String, Term> termCache = new LinkedHashMap<String, Term>() {
		protected boolean removeEldestEntry(Map.Entry<String, Term> eldest) {
			return size() > 1024;
		}
	};

	// language for I10N
	private LANGUAGE language;

	protected Term getTermByWord(String word, boolean create) {
		synchronized (this) {
			Term cachedTerm = termCache.get(word);
			if (cachedTerm != null)
				return cachedTerm;

			Query query = db.query();
			query.constrain(Term.class);
			query.descend("word").constrain(word);
			@SuppressWarnings("unchecked")
			ObjectSet<Term> set = query.execute();

			if (set.hasNext()) {
				cachedTerm = set.next();
				termCache.put(word, cachedTerm);

				return cachedTerm;
			} else if (create) {
				cachedTerm = new Term(word);
				termCache.put(word, cachedTerm);
				db.store(cachedTerm);

				return cachedTerm;
			} else
				return null;
		}
	}

	public String getString(String key) {
		// TODO return a translated string
		return key;
	}

	public void setLanguage(LANGUAGE newLanguage) {
		language = newLanguage;
	}
}
