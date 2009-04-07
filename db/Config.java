/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider.db;

import plugins.XMLSpider.org.garret.perst.Persistent;
import plugins.XMLSpider.org.garret.perst.Storage;
import freenet.node.RequestStarter;
import freenet.support.Logger;

import java.util.Calendar;

public class Config extends Persistent implements Cloneable {
	/**
	 * Directory where the generated indices are stored
	 */
	private String indexDir;
	private int indexMaxEntries;
	private int indexSubindexMaxSize;

	private String indexTitle;
	private String indexOwner;
	private String indexOwnerEmail;

	private int maxShownURIs;
	private int maxParallelRequestsWorking;
	private int maxParallelRequestsNonWorking;
	private int beginWorkingPeriod; // Between 0 and 23
	private int endWorkingPeriod; // Between 0 and 23
	private String[] badlistedExtensions;
	private short requestPriority;

	private boolean debug;

	public Config() {
	}

	public Config(Storage storage) {
		indexDir = "myindex7/";
		indexMaxEntries = 4000;
		indexSubindexMaxSize = 1 * 1024 * 1024;

		indexTitle = "XMLSpider index";
		indexOwner = "Freenet";
		indexOwnerEmail = "(nil)";

		maxShownURIs = 15;

		maxParallelRequestsWorking = 100;
		maxParallelRequestsNonWorking = 100;
		beginWorkingPeriod = 23;
		endWorkingPeriod = 7;

		badlistedExtensions = new String[] { //
				".ico", ".bmp", ".png", ".jpg", ".jpeg", ".gif", // image
				".zip", ".jar", ".gz", ".bz2", ".rar", // archive
		        ".7z", ".rar", ".arj", ".rpm", ".deb", //
		        ".xpi", ".ace", ".cab", ".lza", ".lzh", //
		        ".ace", ".exe", ".iso", // binary
		        ".mpg", ".ogg", "ogv", ".mp3", ".avi", ".wv", ".swf", ".wmv", ".mkv", ".flac", ".ogm", // media
		        ".css", ".sig", ".gml", ".df", "cbr", ".gf", ".pdf" // other
		};

		requestPriority = RequestStarter.IMMEDIATE_SPLITFILE_PRIORITY_CLASS;

		storage.makePersistent(this);
	}

	public synchronized Config clone() {
		try {
			Config config = (Config) super.clone();
			assert !config.isPersistent();
			return config;
		} catch (CloneNotSupportedException e) {
			Logger.error(this, "Impossible exception", e);
			throw new RuntimeException(e);
		}
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

	public synchronized void setIndexSubindexMaxSize(int indexSubindexMaxSize) {
		assert !isPersistent();
		this.indexSubindexMaxSize = indexSubindexMaxSize;
	}

	public synchronized int getIndexSubindexMaxSize() {
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

	public synchronized void setMaxParallelRequestsWorking(int maxParallelRequests) {
		assert !isPersistent();
		this.maxParallelRequestsWorking = maxParallelRequests;
	}

	public synchronized int getMaxParallelRequestsWorking() {
		return maxParallelRequestsWorking;
	}

	public synchronized void setMaxParallelRequestsNonWorking(int maxParallelRequests) {
		assert !isPersistent();
		this.maxParallelRequestsNonWorking = maxParallelRequests;
	}

	public synchronized int getMaxParallelRequestsNonWorking() {
		return maxParallelRequestsNonWorking;
	}

	public synchronized int getMaxParallelRequests() {
		int actualHour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
		Boolean isWorking = true;

		if(this.getBeginWorkingPeriod() < this.getEndWorkingPeriod()) {
			// Midnight isn't in the interval.
			//        m            M
			// 0 -----|############|----- 24
			isWorking = (actualHour > this.getBeginWorkingPeriod() && actualHour < this.getEndWorkingPeriod());
		} else {
			// Midnight is in the interval.
			//        M            m
			// 0 #####|------------|##### 24
			isWorking = (actualHour > this.getBeginWorkingPeriod() || actualHour < this.getEndWorkingPeriod());
		}

		if(isWorking) {
			return this.getMaxParallelRequestsWorking();
		} else {
			return this.getMaxParallelRequestsNonWorking();
		}
	}

	public synchronized void setBeginWorkingPeriod(int beginWorkingPeriod) {
		assert !isPersistent();
		this.beginWorkingPeriod = beginWorkingPeriod;
	}

	public synchronized int getBeginWorkingPeriod() {
		return beginWorkingPeriod;
	}

	public synchronized void setEndWorkingPeriod(int endWorkingPeriod) {
		assert !isPersistent();
		this.endWorkingPeriod = endWorkingPeriod;
	}

	public synchronized int getEndWorkingPeriod() {
		return endWorkingPeriod;
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
