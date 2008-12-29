package plugins.XMLSpider.db;

import java.util.Iterator;
import java.util.List;

import plugins.XMLSpider.org.garret.perst.FieldIndex;
import plugins.XMLSpider.org.garret.perst.Key;
import plugins.XMLSpider.org.garret.perst.Persistent;
import plugins.XMLSpider.org.garret.perst.SortedCollection;
import plugins.XMLSpider.org.garret.perst.Storage;
import freenet.keys.FreenetURI;

public class PerstRoot extends Persistent {
	protected FieldIndex<Term> md5Term;
	protected FieldIndex<Term> wordTerm;

	protected FieldIndex<Page> idPage;
	protected FieldIndex<Page> uriPage;
	protected SortedCollection<Page> queuedPages;
	protected SortedCollection<Page> failedPages;
	protected SortedCollection<Page> succeededPages;
	
	private Config config;

	public PerstRoot() {
	}

	public static PerstRoot createRoot(Storage storage) {
		PerstRoot root = new PerstRoot();

		root.md5Term = storage.createFieldIndex(Term.class, "md5", true);
		root.wordTerm = storage.createFieldIndex(Term.class, "word", true);

		root.idPage = storage.createFieldIndex(Page.class, "id", true);
		root.uriPage = storage.createFieldIndex(Page.class, "uri", true);
		root.queuedPages = storage.<Page> createSortedCollection(new PageTimeStampComparator(), false);
		root.failedPages = storage.<Page> createSortedCollection(new PageTimeStampComparator(), false);
		root.succeededPages = storage.<Page> createSortedCollection(new PageTimeStampComparator(), false);
		
		
		root.config = new Config(storage);
		
		storage.setRoot(root);
		
		return root;
	}

	public synchronized Term getTermByWord(String word, boolean create) {
		Term term = wordTerm.get(new Key(word));

		if (create && term == null) {
			term = new Term(word, getStorage());
			md5Term.add(term);
			wordTerm.add(term);
		}

		return term;
	}

	public synchronized Iterator<Term> getTermIterator() {
		return md5Term.iterator();
	}
	
	public synchronized List<Term> getTermList() {
		return md5Term.getList(null, null);
	}

	public synchronized int getTermCount() {
		return md5Term.size();
	}
	
	public synchronized Page getPageByURI(FreenetURI uri, boolean create, String comment) {
		Page page = uriPage.get(new Key(uri.toString()));

		if (create && page == null) {
			page = new Page(uri.toString(), comment, getStorage());

			idPage.append(page);
			uriPage.put(page);
			queuedPages.add(page);
		}

		return page;
	}

	public Page getPageById(long id) {
		Page page = idPage.get(id);
		return page;
	}
	
	SortedCollection<Page> getPageCollection(Status status) {
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

	public synchronized Iterator<Page> getPages(Status status) {
		return getPageCollection(status).iterator();
	}
	
	public synchronized int getPageCount(Status status) {
		return getPageCollection(status).size();
	}

	public void setConfig(Config config) {
	    this.config = config;
	    modify();
    }

	public Config getConfig() {
	    return config;
    }
}
