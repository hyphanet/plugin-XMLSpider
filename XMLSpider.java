/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package plugins.XMLSpider;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import plugins.XMLSpider.db.Config;
import plugins.XMLSpider.db.Page;
import plugins.XMLSpider.db.PerstRoot;
import plugins.XMLSpider.db.Status;
import plugins.XMLSpider.db.Term;
import plugins.XMLSpider.db.TermPosition;
import plugins.XMLSpider.org.garret.perst.Storage;
import plugins.XMLSpider.org.garret.perst.StorageFactory;
import plugins.XMLSpider.web.WebInterface;
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
	public Config getConfig() {
		// always return a clone, never allow changing directly
		return root.getConfig().clone();
	}

	// Set config asynchronously
	public void setConfig(Config config) {
		callbackExecutor.execute(new SetConfigCallback(config));
	}

	/** Document ID of fetching documents */
	protected Map<Page, ClientGetter> runningFetch = Collections.synchronizedMap(new HashMap<Page, ClientGetter>());

	/**
	 * Lists the allowed mime types of the fetched page. 
	 */
	protected Set<String> allowedMIMETypes;	
	
	static int version = 33;
	public static final String pluginName = "XML spider " + version;

	public String getVersion() {
		return version + " r" + Version.getSvnRevision();
	}
	
	private FetchContext ctx;
	private boolean stopped = true;

	private NodeClientCore core;
	private PageMaker pageMaker;	
	private PluginRespirator pr;
	
	/**
	 * Adds the found uri to the list of to-be-retrieved uris. <p>Every usk uri added as ssk.
	 * @param uri the new uri that needs to be fetched for further indexing
	 */
	public void queueURI(FreenetURI uri, String comment, boolean force) {
		try {
			String sURI = uri.toString();
			for (String ext : root.getConfig().getBadlistedExtensions())
				if (sURI.endsWith(ext))
					return; // be smart

			if (uri.isUSK()) {
				if (uri.getSuggestedEdition() < 0)
					uri = uri.setSuggestedEdition((-1) * uri.getSuggestedEdition());
				try {
					uri = ((USK.create(uri)).getSSK()).getURI();
					(ctx.uskManager).subscribe(USK.create(uri), this, false, this);
				} catch (Exception e) {
				}
			}

			Page page = root.getPageByURI(uri, true, comment);
			if (force && page.getStatus() != Status.QUEUED) {
				page.setStatus(Status.QUEUED);
				page.setComment(comment);
			}
			
			db.commit();
		} catch (RuntimeException e) {
			db.rollback();
			throw e;
		}
	}

	public void startSomeRequests() {
		ArrayList<ClientGetter> toStart = null;
		synchronized (this) {
			if (stopped)
				return;
			synchronized (runningFetch) {
				int running = runningFetch.size();

				if (running >= root.getConfig().getMaxParallelRequests())
					return;

				// Prepare to start
				toStart = new ArrayList<ClientGetter>(root.getConfig().getMaxParallelRequests() - running);
				root.sharedLockPages(Status.QUEUED);
				try {
					Iterator<Page> it = root.getPages(Status.QUEUED);

					while (running + toStart.size() < root.getConfig().getMaxParallelRequests() && it.hasNext()) {
						Page page = it.next();
						if (runningFetch.containsKey(page))
							continue;

						try {
							ClientGetter getter = makeGetter(page);

							Logger.minor(this, "Starting " + getter + " " + page);
							toStart.add(getter);
							runningFetch.put(page, getter);
						} catch (MalformedURLException e) {
							Logger.error(this, "IMPOSSIBLE-Malformed URI: " + page, e);
							page.setStatus(Status.FAILED);
						}
					}
				} finally {
					root.unlockPages(Status.QUEUED);
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

	private class ClientGetterCallback implements ClientCallback {
		final Page page;
		Status status; // for debug

		public ClientGetterCallback(Page page) {
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
		ClientGetter getter = new ClientGetter(new ClientGetterCallback(page),
				core.requestStarters.chkFetchScheduler,
		        core.requestStarters.sskFetchScheduler, new FreenetURI(page.getURI()), ctx,
		        getPollingPriorityProgress(), this, null, null);
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

	public synchronized void scheduleMakeIndex() {
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
				
				db.gc();
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
		
	// Set config asynchronously 
	protected class SetConfigCallback implements Runnable {
		private Config config;

		SetConfigCallback(Config config) {
			this.config = config;
		}

		public void run() {
			synchronized (root) {
				root.getConfig().setValue(config);
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
			else if (r instanceof SetConfigCallback)
				return 3;

			return -1;
		}
	}

	// this is java.util.concurrent.Executor, not freenet.support.Executor
	// always run with one thread --> more thread cause contention and slower!
	public ThreadPoolExecutor callbackExecutor = new ThreadPoolExecutor( //
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
		ClientMetadata cm = result.getMetadata();
		Bucket data = result.asBucket();
		String mimeType = cm.getMIMEType();
		
		try {
			/*
			 * instead of passing the current object, the pagecallback object for every page is
			 * passed to the content filter this has many benefits to efficiency, and allows us to
			 * identify trivially which page is being indexed. (we CANNOT rely on the base href
			 * provided).
			 */
			PageCallBack pageCallBack = new PageCallBack(page);
			Logger.minor(this, "Successful: " + uri + " : " + page.getId());

			ContentFilter.filter(data, new NullBucketFactory(), mimeType, uri.toURI("http://127.0.0.1:8888/"),
			        pageCallBack);
			page.setStatus(Status.SUCCEEDED);
			db.commit();

			Logger.minor(this, "Filtered " + uri + " : " + page.getId());
		} catch (UnsafeContentTypeException e) {
			page.setStatus(Status.SUCCEEDED);
			db.commit();

			Logger.minor(this, "UnsafeContentTypeException " + uri + " : " + page.getId(), e);
			return; // Ignore
		} catch (IOException e) {
			db.rollback();
			Logger.error(this, "Bucket error?: " + e, e);
		} catch (URISyntaxException e) {
			db.rollback();
			Logger.error(this, "Internal error: " + e, e);			
		} catch (RuntimeException e) {
			db.rollback();
			Logger.error(this, "Runtime Exception: " + e, e);		
			throw e;
		} finally {
			data.free();

			synchronized (this) {
				runningFetch.remove(page);
			}
			if (!stopped)
				startSomeRequests();
		}
	}

	protected void onFailure(FetchException fe, ClientGetter state, Page page) {
		Logger.minor(this, "Failed: " + page + " : " + state, fe);

		synchronized (this) {
			if (stopped)
				return;
			
			try {
				synchronized (page) {
					if (fe.newURI != null) {
						// redirect, mark as succeeded
						queueURI(fe.newURI, "redirect from " + state.getURI(), false);
						page.setStatus(Status.SUCCEEDED);
					} else if (fe.isFatal()) {
						// too many tries or fatal, mark as failed
						page.setStatus(Status.FAILED);
					} else {
						// requeue at back
						page.setStatus(Status.QUEUED);
					}
				}
				db.commit();
			} catch (RuntimeException e) {
				db.rollback();
				throw e;
			} finally {
				runningFetch.remove(page);
			}
		}

		startSomeRequests();
	} 

	private boolean writingIndex;
	private boolean writeIndexScheduled;

	protected IndexWriter indexWriter;
	
	public IndexWriter getIndexWriter() {
		return indexWriter;
	}
	
	public void terminate(){
		Logger.normal(this, "XMLSpider terminating");

		synchronized (this) {
			try {
				Runtime.getRuntime().removeShutdownHook(exitHook);
			} catch (IllegalStateException e) {
				// shutting down, ignore
			}
			stopped = true;
			
			for (Map.Entry<Page, ClientGetter> me : runningFetch.entrySet()) {
				ClientGetter getter = me.getValue();
				Logger.minor(this, "Canceling request" + getter);
				getter.cancel();
			}
			runningFetch.clear();
			callbackExecutor.shutdownNow();
		}
		try { callbackExecutor.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException e) {}
		try { db.close(); } catch (Exception e) {}

		Logger.normal(this, "XMLSpider terminated");
	}

	public class ExitHook implements Runnable {
		public void run() {
			Logger.normal(this, "XMLSpider exit hook called");
			terminate();
		}
	}

	private Thread exitHook = new Thread(new ExitHook());

	public synchronized void runPlugin(PluginRespirator pr) {
		this.core = pr.getNode().clientCore;
		this.pr = pr;
		pageMaker = pr.getPageMaker();
		
		Runtime.getRuntime().addShutdownHook(exitHook);

		/* Initialize Fetch Context */
		this.ctx = pr.getHLSimpleClient().getFetchContext();
		ctx.maxSplitfileBlockRetries = 1;
		ctx.maxNonSplitfileRetries = 1;
		ctx.maxTempLength = 2 * 1024 * 1024;
		ctx.maxOutputLength = 2 * 1024 * 1024;
		allowedMIMETypes = new HashSet<String>();
		allowedMIMETypes.add("text/html");
		allowedMIMETypes.add("text/plain");
		allowedMIMETypes.add("application/xhtml+xml");
		ctx.allowedMIMETypes = new HashSet<String>(allowedMIMETypes);

		stopped = false;

		// Initial Database
		db = initDB();
		
		indexWriter = new IndexWriter(this);
		webInterface = new WebInterface(this);

		pr.getNode().executor.execute(new Runnable() {
			public void run() {
				// Add Bookmarks
				FreenetURI[] initialURIs = core.getBookmarkURIs();
				for (int i = 0; i < initialURIs.length; i++)
					queueURI(initialURIs[i], "bookmark", false);

				try{
					Thread.sleep(30 * 1000); // Let the node start up
				} catch (InterruptedException e){}
				startSomeRequests();
			}
		}, "Spider Plugin Starter");
	}

	private WebInterface webInterface;
	
	public String handleHTTPGet(HTTPRequest request) throws PluginHTTPException {
		return webInterface.handleHTTPGet(request);
	}

	public String handleHTTPPost(HTTPRequest request) throws PluginHTTPException {
		return webInterface.handleHTTPPost(request);
	}

	/**
	 * creates the callback object for each page.
	 *<p>Used to create inlinks and outlinks for each page separately.
	 * @author swati
	 *
	 */
	public class PageCallBack implements FoundURICallback{
		protected final Page page;
		
		protected final boolean logDEBUG = Logger.shouldLog(Logger.DEBUG, this); // per instance, allow changing on the fly

		PageCallBack(Page page) {
			this.page = page; 
		}

		public void foundURI(FreenetURI uri){
			// Ignore
		}

		public void foundURI(FreenetURI uri, boolean inline){
			if (stopped)
				throw new RuntimeException("plugin stopping");
			if (logDEBUG)
				Logger.debug(this, "foundURI " + uri + " on " + page);
			queueURI(uri, "Added from " + page.getURI(), false);
		}

		protected Integer lastPosition = null;

		public void onText(String s, String type, URI baseURI){
			if (stopped)
				throw new RuntimeException("plugin stopping");
			if (logDEBUG)
				Logger.debug(this, "onText on " + page.getId() + " (" + baseURI + ")");

			if ("title".equalsIgnoreCase(type) && (s != null) && (s.length() != 0) && (s.indexOf('\n') < 0)) {
				/*
				 * title of the page 
				 */
				page.setPageTitle(s);
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
				try{
					if(type == null)
						addWord(word, lastPosition + i);
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
			if (logDEBUG)
				Logger.debug(this, "addWord on " + page.getId() + " (" + word + "," + position + ")");
			
			if (word.length() < 3)
				return;
			Term term = getTermByWord(word, true);
			TermPosition termPos = page.getTermPosition(term);
			termPos.addPositions(position);
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
		return (short) Math.min(RequestStarter.MINIMUM_PRIORITY_CLASS, root.getConfig().getRequestPriority() + 1);
	}

	public short getPollingPriorityProgress() {
		return root.getConfig().getRequestPriority();
	}

	protected Storage db;
	protected PerstRoot root;

	/**
	 * Initializes Database
	 */
	private Storage initDB() {
		Storage db = StorageFactory.getInstance().createStorage();
		db.setProperty("perst.object.cache.init.size", 8192);
		db.setProperty("perst.alternative.btree", true);
		db.setProperty("perst.string.encoding", "UTF-8");
		db.setProperty("perst.concurrent.iterator", true);

		db.open("XMLSpider-" + version + ".dbs");

		root = (PerstRoot) db.getRoot();
		if (root == null)
			root = PerstRoot.createRoot(db);

		return db;
	}
	
	public PerstRoot getDbRoot() {
		return root;
	}

	protected Page getPageByURI(FreenetURI uri) {
		return root.getPageByURI(uri, false, null);
	}

	protected Page getPageById(long id) {
		return root.getPageById(id);
	}

	// language for I10N
	private LANGUAGE language;

	protected Term getTermByWord(String word, boolean create) {
		return root.getTermByWord(word, create);
	}

	public String getString(String key) {
		// TODO return a translated string
		return key;
	}

	public void setLanguage(LANGUAGE newLanguage) {
		language = newLanguage;
	}

	public PageMaker getPageMaker() {
		return pageMaker;
	}
	
	public List<Page> getRunningFetch() {
		synchronized (runningFetch) {
			return new ArrayList<Page>(runningFetch.keySet());
		}
	}

	public boolean isWriteIndexScheduled() {
		return writeIndexScheduled;
	}

	public boolean isWritingIndex() {
		return writingIndex;
	}

	public PluginRespirator getPluginRespirator() {
		return pr;
	}
}
