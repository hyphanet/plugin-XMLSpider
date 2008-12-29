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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import plugins.XMLSpider.web.WebInterface;

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
	private Config config;

	public Config getConfig() {
		// always return a clone, never allow changing directly
		return config.clone();
	}

	// Set config asynchronously
	public void setConfig(Config config) {
		callbackExecutor.execute(new SetConfigCallback(config));
	}
	
	public synchronized long getNextPageId() {
		long x = maxPageId.incrementAndGet();
		db.store(maxPageId);
		return x;
	}

	/** Document ID of fetching documents */
	protected Map<Page, ClientGetter> runningFetch = Collections.synchronizedMap(new HashMap<Page, ClientGetter>());

	protected MaxPageId maxPageId;

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
		String sURI = uri.toString();
		for (String ext : config.getBadlistedExtensions())
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
	protected long lastPrefetchedTimeStamp = -1; 
	
	public void startSomeRequests() {
		ArrayList<ClientGetter> toStart = null;
		synchronized (this) {
			if (stopped)
				return;
			synchronized (runningFetch) {
				int running = runningFetch.size();

				if (running >= config.getMaxParallelRequests())
					return;

				// prefetch 2 * config.getMaxParallelRequests() entries
				if (queuedRequestCache.isEmpty()) {
					Query query = db.query();
					query.constrain(Page.class);
					query.descend("status").constrain(Status.QUEUED);
					if (lastPrefetchedTimeStamp != -1) {
						query.descend("lastChange").constrain(lastPrefetchedTimeStamp - 1000).greater();
						query.descend("lastChange").constrain(lastPrefetchedTimeStamp + 1800 * 1000).smaller();
					}						
					query.descend("lastChange").orderAscending();
					@SuppressWarnings("unchecked")
					ObjectSet<Page> queuedSet = query.execute();
					
					System.out.println("lastPrefetchedTimeStamp=" + lastPrefetchedTimeStamp + ", BLAR = "
					        + queuedSet.size());
					if (lastPrefetchedTimeStamp != -1 && queuedSet.isEmpty()) {
						lastPrefetchedTimeStamp = -1;
						startSomeRequests();
						return;
					}

					while (queuedRequestCache.size() < config.getMaxParallelRequests() * 2 && queuedSet.hasNext()) {
						Page page = queuedSet.next();
						assert page.status == Status.QUEUED;
						if (!runningFetch.containsKey(page)) {
							queuedRequestCache.add(page);
							
							if (page.lastChange > lastPrefetchedTimeStamp)
								lastPrefetchedTimeStamp = page.lastChange;
						}
					}
				}

				// perpare to start
				toStart = new ArrayList<ClientGetter>(config.getMaxParallelRequests() - running);
				Iterator<Page> it = queuedRequestCache.iterator();

				while (running + toStart.size() < config.getMaxParallelRequests() && it.hasNext()) {
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
		        core.requestStarters.sskFetchScheduler, new FreenetURI(page.uri), ctx, config.getRequestPriority(),
		        this,
		        null, null);
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
			synchronized (this) {
				XMLSpider.this.config.setValue(config);
				db.store(XMLSpider.this.config);
				db.commit();
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

		try {
			ClientMetadata cm = result.getMetadata();
			Bucket data = result.asBucket();
			String mimeType = cm.getMIMEType();

			/*
			 * instead of passing the current object, the pagecallback object for every page is
			 * passed to the content filter this has many benefits to efficiency, and allows us to
			 * identify trivially which page is being indexed. (we CANNOT rely on the base href
			 * provided).
			 */
			PageCallBack pageCallBack = new PageCallBack(page);
			Logger.minor(this, "Successful: " + uri + " : " + page.id);

			try {
				ContentFilter.filter(data, new NullBucketFactory(), mimeType, uri.toURI("http://127.0.0.1:8888/"),
				        pageCallBack);
				pageCallBack.store();

				synchronized (this) {
					page.status = Status.SUCCEEDED;
					page.lastChange = System.currentTimeMillis();
					db.store(page);
					db.commit();
				}
				Logger.minor(this, "Filtered " + uri + " : " + page.id);
			} catch (UnsafeContentTypeException e) {
				Logger.minor(this, "UnsafeContentTypeException " + uri + " : " + page.id, e);
				synchronized (this) {
					page.status = Status.SUCCEEDED;
					page.lastChange = System.currentTimeMillis();
					db.store(page);
					db.commit();
				}
				return; // Ignore
			} catch (IOException e) {
				db.rollback();
				Logger.error(this, "Bucket error?: " + e, e);
			} catch (URISyntaxException e) {
				db.rollback();
				Logger.error(this, "Internal error: " + e, e);
			} finally {
				data.free();
			}
		} catch (RuntimeException e) {
			db.rollback();
			throw e;
		} finally {
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
		try { db.rollback(); } catch (Exception e) {}
		try { db.close(); } catch (Exception e) {}

		synchronized (this) {
			termCache.clear();
		}

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
		
		// Load Config
		{
			Query query = db.query();
			query.constrain(Config.class);
			@SuppressWarnings("unchecked")
			ObjectSet<Config> set = query.execute();

			if (set.hasNext())
				config = set.next();
			else {
				config = new Config(true);
				db.store(config);
				db.commit();
			}
		}	
		
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

		PageCallBack(Page page) {
			this.page = page;
		}

		public void foundURI(FreenetURI uri){
			// Ignore
		}

		public void foundURI(FreenetURI uri, boolean inline){
			if (stopped) return;
			Logger.debug(this, "foundURI " + uri + " on " + page);
			queueURI(uri, "Added from " + page.uri, false);
		}

		protected Integer lastPosition = null;

		public void onText(String s, String type, URI baseURI){
			if (stopped) return;

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
			if (word.length() < 3)
				return;
			Term term = getTermByWord(word, true);
			TermPosition termPos = getTermPosition(term);

			synchronized (termPos) {
				int[] newPositions = new int[termPos.positions.length + 1];
				System.arraycopy(termPos.positions, 0, newPositions, 0, termPos.positions.length);
				newPositions[termPos.positions.length] = position;

				termPos.positions = newPositions;
			}
		}
		
		protected Map<Term, TermPosition> termPosCache = new HashMap<Term, TermPosition>();

		public void store() {
			// Delete existing TermPosition
			Query query = db.query();
			query.constrain(TermPosition.class);
			query.descend("pageId").constrain(page.id);
			@SuppressWarnings("unchecked")
			ObjectSet<TermPosition> set = query.execute();
			for (TermPosition tp : set) {
				assert tp.pageId == page.id;
				db.delete(tp);
			}
			
			for (TermPosition tp : termPosCache.values())
				db.store(tp);
			termPosCache.clear();
		}

		protected TermPosition getTermPosition(Term term) {
			TermPosition cachedTermPos = termPosCache.get(term);
			if (cachedTermPos != null)
				return cachedTermPos;

			cachedTermPos = new TermPosition();
			cachedTermPos.word = term.word;
			cachedTermPos.pageId = page.id;
			cachedTermPos.positions = new int[0];

			termPosCache.put(term, cachedTermPos);
			return cachedTermPos;
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
		return (short) Math.min(RequestStarter.MINIMUM_PRIORITY_CLASS, config.getRequestPriority() + 1);
	}

	public short getPollingPriorityProgress() {
		return config.getRequestPriority();
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
		cfg.objectClass(MaxPageId.class).callConstructor(true);
		cfg.objectClass(Config.class).callConstructor(true);

		cfg.activationDepth(3);
		cfg.updateDepth(3);
		cfg.automaticShutDown(false);
		cfg.queries().evaluationMode(QueryEvaluationMode.LAZY);
		cfg.diagnostic().addListener(new DiagnosticToConsole());

		ObjectContainer oc = Db4o.openFile(cfg, "XMLSpider-" + version + ".db4o");

		return oc;
	}
	
	public ObjectContainer getDB() {
		return db;
	}

	protected Page getPageByURI(FreenetURI uri) {
		Query query = db.query();
		query.constrain(Page.class);
		query.descend("uri").constrain(uri.toString());
		@SuppressWarnings("unchecked")
		ObjectSet<Page> set = query.execute();

		if (set.hasNext()) {
			Page page = set.next();
			assert page.uri.equals(uri.toString());
			return page;
		} else
			return null;
	}

	protected Page getPageById(long id) {
		Query query = db.query();
		query.constrain(Page.class);
		query.descend("id").constrain(id);
		@SuppressWarnings("unchecked")
		ObjectSet<Page> set = query.execute();

		if (set.hasNext()) {
			Page page = set.next();
			assert page.id == id;
			return page;
		} else
			return null;
	}

	protected Term getTermByMd5(String md5) {
		Query query = db.query();
		query.constrain(Term.class);
		query.descend("md5").constrain(md5);
		@SuppressWarnings("unchecked")
		ObjectSet<Term> set = query.execute();

		if (set.hasNext()) {
			Term term = set.next();
			assert md5.equals(term.md5);
			return term;
		} else
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
				assert word.equals(cachedTerm.word);
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
