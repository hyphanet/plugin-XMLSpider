package plugins.XMLSpider.db;

import java.util.Iterator;
import java.util.List;

import plugins.XMLSpider.org.garret.perst.FieldIndex;
import plugins.XMLSpider.org.garret.perst.Key;
import plugins.XMLSpider.org.garret.perst.Persistent;
import plugins.XMLSpider.org.garret.perst.Storage;
import freenet.keys.FreenetURI;

public class PerstRoot extends Persistent {
	protected FieldIndex<Term> md5Term;
	protected FieldIndex<Term> wordTerm;

	protected FieldIndex<Page> idPage;
	protected FieldIndex<Page> uriPage;
	protected FieldIndex<Page> queuedPages;
	protected FieldIndex<Page> failedPages;
	protected FieldIndex<Page> succeededPages;

	private Config config;

	public PerstRoot() {
	}

	public static PerstRoot createRoot(Storage storage) {
		PerstRoot root = new PerstRoot();

		root.md5Term = storage.createFieldIndex(Term.class, "md5", true);
		root.wordTerm = storage.createFieldIndex(Term.class, "word", true);

		root.idPage = storage.createFieldIndex(Page.class, "id", true);
		root.uriPage = storage.createFieldIndex(Page.class, "uri", true);
		root.queuedPages = storage.createFieldIndex(Page.class, "lastChange", false);
		root.failedPages = storage.createFieldIndex(Page.class, "lastChange", false);
		root.succeededPages = storage.createFieldIndex(Page.class, "lastChange", false);

		root.config = new Config(storage);

		storage.setRoot(root);

		return root;
	}

	public Term getTermByWord(String word, boolean create) {
		md5Term.exclusiveLock();
		wordTerm.exclusiveLock();
		try {
			Term term = wordTerm.get(new Key(word));

			if (create && term == null) {
				word = new String(word); // force a new instance, prevent referring to the old char[]			
				term = new Term(word, getStorage());
				md5Term.put(term);
				wordTerm.put(term);
			}

			return term;
		} finally {
			wordTerm.unlock();
			md5Term.unlock();
		}
	}

	public Iterator<Term> getTermIterator() {
		md5Term.sharedLock();
		try {
			return md5Term.iterator();
		} finally {
			md5Term.unlock();
		}
	}

	public List<Term> getTermList() {
		md5Term.sharedLock();
		try {
			return md5Term.getList(null, null);
		} finally {
			md5Term.unlock();
		}
	}

	public int getTermCount() {
		md5Term.sharedLock();
		try {
			return md5Term.size();
		} finally {
			md5Term.unlock();
		}
	}

	public Page getPageByURI(FreenetURI uri, boolean create, String comment) {
		idPage.exclusiveLock();
		uriPage.exclusiveLock();
		queuedPages.exclusiveLock();
		try {
			Page page = uriPage.get(new Key(uri.toString()));

			if (create && page == null) {
				page = new Page(uri.toString(), comment, getStorage());

				idPage.append(page);
				uriPage.put(page);
				queuedPages.put(page);
			}

			return page;
		} finally {
			queuedPages.unlock();
			uriPage.unlock();
			idPage.unlock();
		}
	}

	public Page getPageById(long id) {
		idPage.sharedLock();
		try {
			Page page = idPage.get(id);
			return page;
		} finally {
			idPage.unlock();
		}
	}

	FieldIndex<Page> getPageIndex(Status status) {
		switch (status) {
		case FAILED:
			return failedPages;
		case QUEUED:
			return queuedPages;
		case SUCCEEDED:
			return succeededPages;
		default:
			return null;
		}
	}

	public Iterator<Page> getPages(Status status) {
		FieldIndex<Page> index = getPageIndex(status);
		index.sharedLock();
		try {
			return index.iterator();
		} finally {
			index.unlock();
		}
	}

	public int getPageCount(Status status) {
		FieldIndex<Page> index = getPageIndex(status);
		index.sharedLock();
		try {
			return index.size();
		} finally {
			index.unlock();
		}
	}

	public synchronized void setConfig(Config config) {		
		this.config = config;
		modify();
	}

	public synchronized Config getConfig() {
		return config;
	}
}
