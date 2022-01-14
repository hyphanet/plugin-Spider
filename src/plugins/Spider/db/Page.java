/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.Spider.db;

import java.util.Date;

import freenet.support.Logger;
import plugins.Spider.org.garret.perst.FieldIndex;
import plugins.Spider.org.garret.perst.IPersistentMap;
import plugins.Spider.org.garret.perst.Persistent;
import plugins.Spider.org.garret.perst.Storage;
import plugins.Spider.org.garret.perst.StorageError;

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

	public Page() {
	}

	Page(String uri, String comment, Storage storage) {
		this.uri = uri;
		this.comment = comment;
		this.status = Status.NEW;
		this.lastChange = System.currentTimeMillis();
		
		storage.makePersistent(this);
	}
	
	public synchronized void setStatus(Status status) {
		Logger.debug(this, "New status " + status + " for " + this);
		preModify();
		this.status = status;
		postModify();
	}

	public Status getStatus() {
		return status;
	}

	public synchronized void setComment(String comment) {
		Logger.debug(this, "New comment for " + this);
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
		Logger.debug(this, "New page title for " + this);
		preModify();
		this.pageTitle = pageTitle;
		postModify();
	}

	public String getPageTitle() {
		return pageTitle;
	}

	public String getLastChangeAsString() {
		return new Date(lastChange).toString();
	}

	public Date getLastChange() {
		return new Date(lastChange);
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
			FieldIndex<Page> coll = root.getPageIndex(status);
			coll.exclusiveLock();
			try {
				coll.remove(this);
			} catch (StorageError e) {
				if(e.getErrorCode() == StorageError.KEY_NOT_FOUND) {
					// No serious consequences, so just log it, rather than killing the whole thing.
					Logger.error(this, "Page: Key not found in index: "+this, e);
					System.err.println("Page: Key not found in index: "+this);
					e.printStackTrace();
				} else {
					Logger.error(this, "remove from index " + status + " failed", e);
					throw e;
				}
			} finally {
				coll.unlock();
			}
		}
	}

	private void postModify() {
		lastChange = System.currentTimeMillis();
		
		modify();

		Storage storage = getStorage();

		if (storage != null) {
			PerstRoot root = (PerstRoot) storage.getRoot();
			FieldIndex<Page> coll = root.getPageIndex(status);
			coll.exclusiveLock();
			try {
				coll.put(this);
			} finally {
				coll.unlock();
			}
		}
	}

	/**
	 * Called when we find the page in the wrong list.
	 * 
	 * This should never happen but it has and is a major problem since it
	 * locks up the search.
	 */
	public void pageFoundInWrongList() {
		Storage storage = getStorage();

		if (storage != null) {
			PerstRoot root = (PerstRoot) storage.getRoot();
			Logger.error(this, "Page " + this + " found in wrong list. Will remove from all lists and put back.");
			for (Status status : Status.values()) {
				FieldIndex<Page> coll = root.getPageIndex(status);
				coll.exclusiveLock();
				try {
					coll.remove(this);
					Logger.minor(this, "Page " + this + " was removed from " + status);
				} catch (StorageError e) {
					if(e.getErrorCode() == StorageError.KEY_NOT_FOUND) {
						// This is the normal case.
					} else {
						Logger.error(this, "Error in storage when removing " + this + " from " + status + ".", e);
					}
				} finally {
					coll.unlock();
				}
			}
			FieldIndex<Page> coll = root.getPageIndex(status);
			coll.exclusiveLock();
			try {
				coll.put(this);
			} finally {
				coll.unlock();
			}
		}
	}
}
