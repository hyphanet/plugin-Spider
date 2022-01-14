package plugins.Spider.db;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import plugins.Spider.org.garret.perst.FieldIndex;
import plugins.Spider.org.garret.perst.Key;
import plugins.Spider.org.garret.perst.Persistent;
import plugins.Spider.org.garret.perst.Storage;
import freenet.keys.FreenetURI;
import freenet.support.Logger;

public class PerstRoot extends Persistent {

	protected FieldIndex<Page> idPage;
	protected FieldIndex<Page> uriPage;
	Map<Status, FieldIndex<Page>> statusPages = new HashMap<Status, FieldIndex<Page>>();

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
		for (Status status : Status.values()) {
			statusPages.put(status, storage.<Page>createFieldIndex(Page.class, "lastChange", true));
		}

		config = new Config(storage);
	}
	
	public Page getPageByURI(FreenetURI uri, boolean create, String comment) {
		idPage.exclusiveLock();
		uriPage.exclusiveLock();
		statusPages.get(Status.NEW).exclusiveLock();
		try {
			Page page = uriPage.get(new Key(uri.toString()));

			if (create && page == null) {
				Logger.debug(this, "New page created for " + uri.toString());
				page = new Page(uri.toString(), comment, getStorage());

				idPage.append(page);
				uriPage.put(page);
				statusPages.get(Status.NEW).put(page);
			}

			return page;
		} finally {
			statusPages.get(Status.NEW).unlock();
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
		return statusPages.get(status);
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
