/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.Spider.db;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
	/** suggestedEdition of the page */
	protected long edition;
	/** Title */
	protected String pageTitle;
	/** Status */
	protected Status status;
	/** Last Change Time */
	protected long lastChange;
	/** Last Fetched Time
	 * 
	 * This is for the case when many USK pages are updated more often 
	 * than they are fetched. In that case, this is used to prioritize
	 * the update of older pages over pages that were recently fetched.
	 */
	protected long lastFetched;
	/** Comment, for debugging */
	protected String comment;

	public Page() {
	}

	Page(String uri, long edition, String comment, Storage storage) {
		this.uri = uri;
		this.edition = edition;
		this.comment = comment;
		this.status = Status.NEW;
		this.lastChange = System.currentTimeMillis();
		this.lastFetched = 0L; // 0 means never fetched.
		
		storage.makePersistent(this);
	}

	Page(String uri, String comment, Storage storage) {
		this(uri, 0L, comment, storage);
	}

	public long getEdition() {
		return edition;
	}

	public synchronized void setStatus(long edition, Status status, String comment) {
		List<String> mess = new ArrayList<String>(); 
		if (edition != 0L) {
			mess.add("edition " + edition);
		}
		if (status != null) {
			mess.add("status " + status);
		}
		if (comment != null) {
			mess.add("comment \"" + comment + "\"");
		}
		Logger.debug(this, "New " + String.join(", ", mess) + " for " + this);
		preModify();
		if (edition != 0L) {
			this.edition = edition;
		}
		if (status != null) {
			this.status = status;
		}
		if (comment != null) {
			this.comment = comment;
		}
		postModify();
	}

	public synchronized void setStatus(Status status) {
		setStatus(status, null);
	}

	public synchronized void setStatus(Status status, String comment) {
		setStatus(0, status, comment);
	}

	public Status getStatus() {
		return status;
	}

	public synchronized void setComment(String comment) {
		setStatus(0, null, comment);
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
		this.pageTitle = pageTitle;
		modify();
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

	public void setLastFetched() {
		lastFetched = System.currentTimeMillis();
	}

	public boolean hasBeenFetched() {
		return lastFetched != 0L;
	}

	public Date getLastFetched() {
		return new Date(lastFetched);
	}

	public String getLastFetchedAsString() {
		if (lastFetched > 0L) {
			return new Date(lastFetched).toString();
		}
		return "";
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
		return "[PAGE: id=" + id + ", title=" + pageTitle + ", uri=" + uri + ", edition=" + edition + " status=" + status + ", comment="
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
}
