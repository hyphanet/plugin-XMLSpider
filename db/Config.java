/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider.db;

import plugins.XMLSpider.org.garret.perst.Persistent;
import plugins.XMLSpider.org.garret.perst.Storage;
import freenet.node.RequestStarter;

public class Config extends Persistent implements Cloneable {
	/**
	 * Directory where the generated indices are stored
	 */
	private String indexDir;
	private int indexMaxEntries;
	private long indexSubindexMaxSize;

	private String indexTitle;
	private String indexOwner;
	private String indexOwnerEmail;

	private int maxShownURIs;
	private int maxParallelRequests;
	private String[] badlistedExtensions;
	private short requestPriority;
	
	private boolean debug;

	public Config() {
	}

	public Config(Storage storage) {
		indexDir = "myindex7/";
		indexMaxEntries = 2000;
		indexSubindexMaxSize = 4 * 1024 * 1024;

		indexTitle = "XMLSpider index";
		indexOwner = "Freenet";
		indexOwnerEmail = "(nil)";

		maxShownURIs = 15;

		maxParallelRequests = 100;

		badlistedExtensions = new String[] { //
		".ico", ".bmp", ".png", ".jpg", ".gif", // image
		        ".zip", ".jar", ".gz", ".bz2", ".rar", // archive
		        ".7z", ".rar", ".arj", ".rpm", ".deb", //
		        ".xpi", ".ace", ".cab", ".lza", ".lzh", //
		        ".ace", ".exe", ".iso", // binary
		        ".mpg", ".ogg", ".mp3", ".avi", // media
		        ".css", ".sig" // other
		};

		requestPriority = RequestStarter.IMMEDIATE_SPLITFILE_PRIORITY_CLASS;
		
		storage.makePersistent(this);
	}

	public synchronized void setValue(Config config) {
		synchronized (config) {
			indexDir = config.indexDir;
			indexMaxEntries = config.indexMaxEntries;
			indexSubindexMaxSize = config.indexSubindexMaxSize;

			indexTitle = config.indexTitle;
			indexOwner = config.indexOwner;
			indexOwnerEmail = config.indexOwnerEmail;

			maxShownURIs = config.maxShownURIs;

			maxParallelRequests = config.maxParallelRequests;

			badlistedExtensions = config.badlistedExtensions;

			requestPriority = config.requestPriority;
		}
		
		if (isPersistent())
			modify();
	}

	public synchronized Config clone() {
		Config newConfig = new Config();
		newConfig.setValue(this);
		return newConfig;
	}

	public synchronized void setIndexDir(String indexDir) {
		assert !isPersistent();
		this.indexDir = indexDir;
	}

	public synchronized String getIndexDir() {
		return indexDir;
	}

	public synchronized void setIndexMaxEntries(int indexMaxEntries) {
		assert !isPersistent();
		this.indexMaxEntries = indexMaxEntries;
	}

	public synchronized int getIndexMaxEntries() {
		return indexMaxEntries;
	}

	public synchronized void setIndexSubindexMaxSize(long indexSubindexMaxSize) {
		assert !isPersistent();
		this.indexSubindexMaxSize = indexSubindexMaxSize;
	}

	public synchronized long getIndexSubindexMaxSize() {
		return indexSubindexMaxSize;
	}

	public synchronized void setIndexTitle(String indexTitle) {
		assert !isPersistent();
		this.indexTitle = indexTitle;
	}

	public synchronized String getIndexTitle() {
		return indexTitle;
	}

	public synchronized void setIndexOwner(String indexOwner) {
		assert !isPersistent();
		this.indexOwner = indexOwner;
	}

	public synchronized String getIndexOwner() {
		return indexOwner;
	}

	public synchronized void setIndexOwnerEmail(String indexOwnerEmail) {
		assert !isPersistent();
		this.indexOwnerEmail = indexOwnerEmail;
	}

	public synchronized void setMaxShownURIs(int maxShownURIs) {
		assert !isPersistent();
		this.maxShownURIs = maxShownURIs;
	}

	public synchronized int getMaxShownURIs() {
		return maxShownURIs;
	}

	public synchronized String getIndexOwnerEmail() {
		return indexOwnerEmail;
	}

	public synchronized void setMaxParallelRequests(int maxParallelRequests) {
		assert !isPersistent();
		this.maxParallelRequests = maxParallelRequests;
	}

	public synchronized int getMaxParallelRequests() {
		return maxParallelRequests;
	}

	public synchronized void setBadlistedExtensions(String[] badlistedExtensions) {
		assert !isPersistent();
		this.badlistedExtensions = badlistedExtensions;
	}

	public synchronized String[] getBadlistedExtensions() {
		return badlistedExtensions;
	}

	public synchronized void setRequestPriority(short requestPriority) {
		assert !isPersistent();
		this.requestPriority = requestPriority;
	}

	public synchronized short getRequestPriority() {
		return requestPriority;
	}
	
	public synchronized boolean isDebug() {
		return debug;
	}

	public synchronized void debug(boolean debug) {
		assert !isPersistent();
		this.debug = debug;
	}
}