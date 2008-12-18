/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider;

class Page {
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