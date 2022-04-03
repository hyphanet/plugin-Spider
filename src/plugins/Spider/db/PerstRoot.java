package plugins.Spider.db;

import java.util.Iterator;

import plugins.Spider.org.garret.perst.FieldIndex;
import plugins.Spider.org.garret.perst.Key;
import plugins.Spider.org.garret.perst.Persistent;
import plugins.Spider.org.garret.perst.Storage;
import freenet.keys.FreenetURI;
import freenet.support.Logger;

public class PerstRoot extends Persistent {

	protected FieldIndex<Page> idPage;
	protected FieldIndex<Page> uriPage;
	FieldIndex[] statusPages;

	private Config config;

	public PerstRoot() {
	}

	public static PerstRoot createRoot(Storage storage) {
		PerstRoot root = new PerstRoot();

		root.create(storage);

		storage.setRoot(root);

		return root;
	}

	private void create(Storage storage) {
		idPage = storage.createFieldIndex(Page.class, "id", true);
		uriPage = storage.createFieldIndex(Page.class, "uri", true);
		statusPages = new FieldIndex[Status.values().length];
		for (Status status : Status.values()) {
			statusPages[status.ordinal()] = storage.<Page>createFieldIndex(Page.class, "lastChange", true);
		}

		config = new Config(storage);
	}
	
	/**
	 * Finds or creates pages in the database.
	 * 
	 * @param uri The URI of the page to find.
	 * @param create if true then the page is created if it doesn't exist.
	 * @param comment is only used when create is true.
	 * @return the page.
	 */
	public Page getPageByURI(FreenetURI uri, boolean create, String comment) {
		idPage.exclusiveLock();
		uriPage.exclusiveLock();
		getPageIndex(Status.NEW).exclusiveLock();
		try {
			Page page = uriPage.get(new Key(uri.toString()));

			if (create && page == null) {
				Logger.debug(this, "New page created for " + uri.toString());
				page = new Page(uri.toString(), comment, getStorage());

				idPage.append(page);
				uriPage.put(page);
				getPageIndex(Status.NEW).put(page);
			}

			return page;
		} finally {
			getPageIndex(Status.NEW).unlock();
			uriPage.unlock();
			idPage.unlock();
		}
	}

	/**
	 * Find a page in the database.
	 * @param uri The page to find.
	 * @return null if not found
	 */
	public Page getPageByURI(FreenetURI uri) {
		return getPageByURI(uri, false, null);
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
		return statusPages[status.ordinal()];
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
