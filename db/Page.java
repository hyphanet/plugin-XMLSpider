/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider.db;

import plugins.XMLSpider.org.garret.perst.IPersistentMap;
import plugins.XMLSpider.org.garret.perst.Persistent;
import plugins.XMLSpider.org.garret.perst.SortedCollection;
import plugins.XMLSpider.org.garret.perst.Storage;

public class Page extends Persistent implements Comparable<Page> {
	/** Page Id */
	protected long id;
	/** URI of the page */
	protected String uri;
	/** Title */
	protected String pageTitle;
	/** Status */
	protected Status status;
	/** Last Change Time */
	protected long lastChange;
	/** Comment, for debugging */
	protected String comment;
	/** term.md5 -> TermPosition */
	protected IPersistentMap<String, TermPosition> termPosMap;

	public Page() {
	}

	Page(String uri, String comment, Storage storage) {
		this.uri = uri;
		this.comment = comment;
		this.status = Status.QUEUED;
		this.lastChange = System.currentTimeMillis();
		
		storage.makePersistent(this);
	}
	
	public synchronized void setStatus(Status status) {
		preModify();
		this.status = status;
		postModify();
	}

	public Status getStatus() {
		return status;
	}

	public synchronized void setComment(String comment) {
		preModify();
		this.comment = comment;
		postModify();
	}
	
	public String getComment() {
		return comment;
	}

	public String getURI() {
		return uri;
	}
	
	public long getId() {
		return id;
	}
	
	public void setPageTitle(String pageTitle) {
		preModify();
		this.pageTitle = pageTitle;
		postModify();
	}

	public String getPageTitle() {
		return pageTitle;
	}

	public synchronized TermPosition getTermPosition(Term term) {
		if (termPosMap == null)
			termPosMap = getStorage().createMap(String.class);

		TermPosition tp = termPosMap.get(term.md5);
		if (tp == null) {
			tp = new TermPosition(getStorage());
			term.pageSet.add(this);
		}

		return tp;
	}
	
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

	public int compareTo(Page o) {
		return new Long(id).compareTo(o.id);
	}
	
	private void preModify() {
		Storage storage = getStorage();

		if (storage != null) {
			PerstRoot root = (PerstRoot) storage.getRoot();
			SortedCollection<Page> coll = root.getPageCollection(status);
			coll.remove(this);
		}
	}

	private void postModify() {
		lastChange = System.currentTimeMillis();
		
		modify();

		Storage storage = getStorage();

		if (storage != null) {
			PerstRoot root = (PerstRoot) storage.getRoot();
			SortedCollection<Page> coll = root.getPageCollection(status);
			coll.add(this);
		}
	}
}
