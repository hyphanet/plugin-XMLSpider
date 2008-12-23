/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider;

import freenet.node.RequestStarter;

class Config {
	/**
	 * directory where the generated indices are stored. Needs to be created before it can be used
	 */
	private String indexDir = "myindex7/";
	private int indexMaxEntries = 2000;
	private long indexSubindexMaxSize = 4 * 1024 * 1024;

	private String indexTitle = "XMLSpider index";
	private String indexOwner = "Freenet";
	private String indexOwnerEmail = null;

	private int maxShownURIs = 15;

	// Can have many; this limit only exists to save memory.
	private int maxParallelRequests = 100;

	private String[] badlistedExtensions = new String[] { //
	".ico", ".bmp", ".png", ".jpg", ".gif", // image
	        ".zip", ".jar", ".gz", ".bz2", ".rar", // archive
	        ".7z", ".rar", ".arj", ".rpm", ".deb", ".xpi", ".ace", ".cab", ".lza", ".lzh", ".ace", ".exe", ".iso", // binary
	        ".mpg", ".ogg", ".mp3", ".avi", // media
	        ".css", ".sig" // other
	};

	// Equal to Frost, ARK fetches etc. One step down from Fproxy.
	// Any lower makes it very difficult to debug. Maybe reduce for production - after solving the ARK bugs.
	private short requestPriority = RequestStarter.IMMEDIATE_SPLITFILE_PRIORITY_CLASS;

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