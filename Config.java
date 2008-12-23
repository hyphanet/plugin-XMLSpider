/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider;

import freenet.node.RequestStarter;
import freenet.support.Logger;

class Config implements Cloneable {
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

	public Config() {
	} // for db4o

	public Config(boolean setDefault) {
		if (!setDefault)
			return;

		indexDir = "myindex7/";
		indexMaxEntries = 2000;
		indexSubindexMaxSize = 4 * 1024 * 1024;

		indexTitle = "XMLSpider index";
		indexOwner = "Freenet";
		indexOwnerEmail = null;

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
	}

	public synchronized Config clone() {
		try {
			return (Config) super.clone();
		} catch (CloneNotSupportedException e) {
			Logger.error(this, "impossible:", e);
			throw new RuntimeException(e);
		}
	}

	public synchronized void setIndexDir(String indexDir) {
		this.indexDir = indexDir;
	}

	public synchronized String getIndexDir() {
		return indexDir;
	}

	public synchronized void setIndexMaxEntries(int indexMaxEntries) {
		this.indexMaxEntries = indexMaxEntries;
	}

	public synchronized int getIndexMaxEntries() {
		return indexMaxEntries;
	}

	public synchronized void setIndexSubindexMaxSize(long indexSubindexMaxSize) {
		this.indexSubindexMaxSize = indexSubindexMaxSize;
	}

	public synchronized long getIndexSubindexMaxSize() {
		return indexSubindexMaxSize;
	}

	public synchronized void setIndexTitle(String indexTitle) {
		this.indexTitle = indexTitle;
	}

	public synchronized String getIndexTitle() {
		return indexTitle;
	}

	public synchronized void setIndexOwner(String indexOwner) {
		this.indexOwner = indexOwner;
	}

	public synchronized String getIndexOwner() {
		return indexOwner;
	}

	public synchronized void setIndexOwnerEmail(String indexOwnerEmail) {
		this.indexOwnerEmail = indexOwnerEmail;
	}

	public synchronized void setMaxShownURIs(int maxShownURIs) {
		this.maxShownURIs = maxShownURIs;
	}

	public synchronized int getMaxShownURIs() {
		return maxShownURIs;
	}

	public synchronized String getIndexOwnerEmail() {
		return indexOwnerEmail;
	}

	public synchronized void setMaxParallelRequests(int maxParallelRequests) {
		this.maxParallelRequests = maxParallelRequests;
	}

	public synchronized int getMaxParallelRequests() {
		return maxParallelRequests;
	}

	public synchronized void setBadlistedExtensions(String[] badlistedExtensions) {
		this.badlistedExtensions = badlistedExtensions;
	}

	public synchronized String[] getBadlistedExtensions() {
		return badlistedExtensions;
	}

	public synchronized void setRequestPriority(short requestPriority) {
		this.requestPriority = requestPriority;
	}

	public synchronized short getRequestPriority() {
		return requestPriority;
	}
}