package plugins.Spider.db;

import java.util.Iterator;
import java.util.List;

import plugins.Spider.org.garret.perst.FieldIndex;
import plugins.Spider.org.garret.perst.IterableIterator;
import plugins.Spider.org.garret.perst.Key;
import plugins.Spider.org.garret.perst.Persistent;
import plugins.Spider.org.garret.perst.Storage;
import freenet.keys.FreenetURI;

public class PerstRoot extends Persistent {

	protected FieldIndex<Page> idPage;
	protected FieldIndex<Page> uriPage;
	protected FieldIndex<Page> newPages;
	protected FieldIndex<Page> queuedPages;
	protected FieldIndex<Page> failedPages;
	protected FieldIndex<Page> succeededPages;
	protected FieldIndex<Page> notPushedPages;
	protected FieldIndex<Page> indexedPages;

	private Config config;

	public PerstRoot() {
	}

	public static PerstRoot createRoot(Storage storage) {
		PerstRoot root = new PerstRoot();

		root.idPage = storage.createFieldIndex(Page.class, "id", true);
		root.uriPage = storage.createFieldIndex(Page.class, "uri", true);
		root.newPages = storage.createFieldIndex(Page.class, "lastChange", false);
		root.queuedPages = storage.createFieldIndex(Page.class, "lastChange", false);
		root.failedPages = storage.createFieldIndex(Page.class, "lastChange", false);
		root.succeededPages = storage.createFieldIndex(Page.class, "lastChange", false);
		root.notPushedPages = storage.createFieldIndex(Page.class, "lastChange", false);
		root.indexedPages = storage.createFieldIndex(Page.class, "lastChange", false);

		root.config = new Config(storage);

		storage.setRoot(root);

		return root;
	}
	
	public Page getPageByURI(FreenetURI uri, boolean create, String comment) {
		idPage.exclusiveLock();
		uriPage.exclusiveLock();
		newPages.exclusiveLock();
		try {
			Page page = uriPage.get(new Key(uri.toString()));

			if (create && page == null) {
				page = new Page(uri.toString(), comment, getStorage());

				idPage.append(page);
				uriPage.put(page);
				newPages.put(page);
			}

			return page;
		} finally {
			newPages.unlock();
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
		case NEW:
			return newPages;
		case QUEUED:
			return queuedPages;
		case SUCCEEDED:
			return succeededPages;
		case NOT_PUSHED:
			return notPushedPages;
		case INDEXED:
			return indexedPages;
		default:
			return null;
		}
	}

	public void exclusiveLock(Status status) {
		FieldIndex<Page> index = getPageIndex(status);
		index.exclusiveLock();
	}

	public void sharedLockPages(Status status) {
		FieldIndex<Page> index = getPageIndex(status);
		index.sharedLock();
	}

	public void unlockPages(Status status) {
		FieldIndex<Page> index = getPageIndex(status);
		index.unlock();
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
