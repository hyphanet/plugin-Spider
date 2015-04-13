
package plugins.Spider;

import freenet.keys.FreenetURI;
import freenet.pluginmanager.FredPluginTalker;
import freenet.pluginmanager.PluginNotFoundException;
import freenet.pluginmanager.PluginRespirator;
import freenet.pluginmanager.PluginTalker;
import freenet.support.Logger;
import freenet.support.SimpleFieldSet;
import freenet.support.api.Bucket;
import freenet.support.io.FileBucket;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.TreeMap;
import plugins.Spider.index.TermEntryWriter;
import plugins.Spider.index.TermPageEntry;
import plugins.Spider.db.Status;
import plugins.Spider.org.garret.perst.Storage;

/**
 * Buffer which stores TermPageEntrys as they are found by the Spider. When the
 * buffer's estimated size gets up to bufferMax, the buffer is serialized into a
 * Bucket and sent to the Library
 * 
 *
 * @author MikeB
 */
public class LibraryBuffer implements FredPluginTalker {
	private PluginRespirator pr;
	private long timeStalled = 0;
	private long timeNotStalled = 0;
	private long timeLastNotStalled = System.currentTimeMillis();
	private boolean shutdown;
	private FreenetURI lastURI;
	private boolean badURI;

	private TreeMap<TermPageEntry, TermPageEntry> termPageBuffer = new TreeMap();
	// Garbage collection behaving perversely. Lets try moving stuff into instance members.
	private Collection<TermPageEntry> pushing = null;

	private int bufferUsageEstimate = 0;
	private int bufferMax;

	static final File SAVE_FILE = new File("spider.saved.data");
	
	/** For resetPages */
	private Spider spider;

	synchronized void setBufferSize(int maxSize) {
		if(maxSize <= 0) throw new IllegalArgumentException();
		bufferMax = maxSize;
	}

	/** We only consider sending the data after a file has been parsed, not mid way through. */
	public void maybeSend() {
		boolean push = false;
		synchronized(this) {
			if (bufferMax == 0) return;
			if (bufferUsageEstimate > bufferMax) {
				if(pushing != null) {
					throw new IllegalStateException("Still pushing?!");
				}
				pushing = termPageBuffer.values();
				push = true;
				termPageBuffer = new TreeMap();
				bufferUsageEstimate = 0;
			}
		}
		if(push) sendBuffer();
	}
	
	/**
	 * Increments the estimate by specified amount.
	 * @param increment
	 */
	private synchronized void increaseEstimate(int increment) {
		bufferUsageEstimate += increment;
	}
	
	public synchronized int bufferUsageEstimate() {
		return bufferUsageEstimate;
	}
	

	LibraryBuffer(PluginRespirator pr, Spider spider) {
		this.pr = pr;
		this.spider = spider;
	}
	
	public void start() {
		// Do in a transaction so it gets committed separately.
		spider.db.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
		spider.resetPages(Status.NOT_PUSHED, Status.QUEUED);
		spider.db.endThreadTransaction();
	}

	/**
	 * Takes a TermPageEntry and either returns a TPE of the same term & page
	 * from the buffer or adds the TPE to the buffer and returns it.
	 *
	 * @param newTPE
	 * @return
	 */
	private synchronized TermPageEntry get(TermPageEntry newTPE) {
		if(shutdown) {
			while(true)
				try {
					wait();// Don't add anything more, don't allow the transaction to commit.
					// FIXME throw something instead???
				} catch (InterruptedException e) {
					// Ignore
				} 
		}
		TermPageEntry exTPE = termPageBuffer.get(newTPE);
		if(exTPE==null) {	// TPE is new
			increaseEstimate(newTPE.sizeEstimate());
			termPageBuffer.put(newTPE, newTPE);
			return newTPE;
		} else
			return exTPE;
	}

	/**
	 * Set the title of the
	 * @param termPageEntry
	 * @param s
	 */
	synchronized void setTitle(TermPageEntry termPageEntry, String s) {
		get(termPageEntry).title = s;
	}
	
	synchronized void setRelevance(TermPageEntry termPageEntry, float f) {
		get(termPageEntry).rel = f;
	}

	/**
	 * Puts a term position in the TermPageEntry and increments the bufferUsageEstimate
	 * @param tp
	 * @param position
	 */
	synchronized void addPos(TermPageEntry tp, int position) {
		//Logger.normal(this, "length : "+bufferUsageEstimate+", adding to "+tp);
		get(tp).putPosition(position);
		//Logger.normal(this, "length : "+bufferUsageEstimate+", increasing length "+tp);
		increaseEstimate(4);
	}

	/**
	 * Emptys the buffer into a bucket and sends it to the Library plugin with the command "pushBuffer"
	 *
	 * FIXME : I think there is something wrong with the way it writes to the bucket, I may be using the wrong kind of buffer
	 */
	private void sendBuffer() {
		long tStart = System.currentTimeMillis();
		try {
			Logger.normal(this, "Sending buffer of estimated size "+bufferUsageEstimate+" bytes to Library");
			long totalPagesIndexed = spider.getRoot().getPageCount(Status.INDEXED);
			Bucket bucket = pr.getNode().clientCore.tempBucketFactory.makeBucket(3000000);
			writeToPush(totalPagesIndexed, bucket);
			innerSend(bucket);
			Logger.normal(this, "Buffer successfully sent to Library, size = "+bucket.size());
			// Not a separate transaction, commit with the index updates.
			spider.resetPages(Status.NOT_PUSHED, Status.INDEXED);
		} catch (IOException ex) {
			Logger.error(this, "Could not make bucket to transfer buffer", ex);
		}
		long tEnd = System.currentTimeMillis();
		synchronized(this) {
			timeNotStalled += (tStart - timeLastNotStalled);
			timeLastNotStalled = tEnd;
			timeStalled += (tEnd - tStart);
			if(shutdown) return;
		}
		// Robustness: Send SAVE_FILE *after* sending new data, because *it is already on disk*, whereas the new data is not.
		if(SAVE_FILE.exists()) {
			System.out.println("Restoring data from last time from "+SAVE_FILE);
			Bucket bucket = new FileBucket(SAVE_FILE, true, false, false, true);
			innerSend(bucket);
			System.out.println("Restored data from last time from "+SAVE_FILE);
		}
	}
	
	private synchronized Bucket writeToPush(long totalPagesIndexed, Bucket bucket) throws IOException {
		OutputStream os = bucket.getOutputStream();
		SimpleFieldSet meta = new SimpleFieldSet(true); // Stored with data to make things easier.
		String indexTitle = spider.getConfig().getIndexTitle();
		String indexOwner = spider.getConfig().getIndexOwner();
		String indexOwnerEmail = spider.getConfig().getIndexOwnerEmail();
		if(indexTitle != null)
			meta.putSingle("index.title", indexTitle);
		if(indexOwner != null)
			meta.putSingle("index.owner.name", indexOwner);
		if(indexOwnerEmail != null)
			meta.putSingle("index.owner.email", indexOwnerEmail);
		meta.put("totalPages", totalPagesIndexed);
		meta.writeTo(os);
		for (TermPageEntry termPageEntry : pushing) {
			TermEntryWriter.getInstance().writeObject(termPageEntry, os);
		}
		pushing = null;
		os.close();
		bucket.setReadOnly();
		return bucket;
	}

	private void innerSend(Bucket bucket) {
		SimpleFieldSet sfs = new SimpleFieldSet(true);
		sfs.putSingle("command", "pushBuffer");
		PluginTalker libraryTalker;
		try {
			libraryTalker = pr.getPluginTalker(this, "plugins.Library.Main", "SpiderBuffer");
			libraryTalker.sendSyncInternalOnly(sfs, bucket);
			bucket.free();
		} catch (PluginNotFoundException e) {
			Logger.error(this, "Couldn't connect buffer to Library", e);
		}

	}
	
	public long getTimeStalled() {
		return timeStalled;
	}
	
	public long getTimeNotStalled() {
		return timeNotStalled;
	}

	public void onReply(String pluginname, String indentifier, SimpleFieldSet params, Bucket data) {
		String reply = params.get("reply");
		if("getSpiderURI".equals(reply)) {
			String s = params.get("publicUSK");
			if(s != null) {
				try {
					FreenetURI uri = new FreenetURI(s);
					synchronized(this) {
						lastURI = uri;
						notifyAll();
					}
					return;
				} catch (MalformedURLException e) {
					Logger.error(this, "Got bogus URI: "+s, new Exception("error"));
				}
			}
			synchronized(this) {
				badURI = true;
				notifyAll();
			}
		}
		Logger.error(this, "Ignoring message "+reply);
	}

	public void terminate() {
		synchronized(this) {
			if(shutdown) {
				Logger.error(this, "Shutdown called twice", new Exception("error"));
				return;
			}
			shutdown = true;
			pushing = termPageBuffer.values();
			termPageBuffer = new TreeMap();
			bufferUsageEstimate = 0;
		}
		System.out.println("Writing pending data to "+SAVE_FILE);
		FileBucket bucket = new FileBucket(SAVE_FILE, false, false, false, false);
		long totalPagesIndexed;
		try {
			totalPagesIndexed = spider.getRoot().getPageCount(Status.INDEXED);
		} catch (Throwable t) {
			totalPagesIndexed = -1;
			// FIXME I don't understand why this (ClassNotFoundException) happens, we have not closed the class loader yet.
			System.err.println("Failed to determine page size during writing data to "+SAVE_FILE+": "+t);
			t.printStackTrace();
		}
		try {
			writeToPush(totalPagesIndexed, bucket);
		} catch (IOException e) {
			System.err.println("Failed to write pending data: "+e); // Hopefully no data loss due to NOT_PUSHED vs INDEXED.
			e.printStackTrace();
		}
		System.out.println("Written pending data to "+SAVE_FILE);
	}

	public FreenetURI getURI() {
		SimpleFieldSet sfs = new SimpleFieldSet(true);
		sfs.putSingle("command", "getSpiderURI");
		PluginTalker libraryTalker;
		try {
			libraryTalker = pr.getPluginTalker(this, "plugins.Library.Main", "SpiderBuffer.getURI");
			libraryTalker.sendSyncInternalOnly(sfs, null);
		} catch (PluginNotFoundException e) {
			Logger.error(this, "Couldn't connect buffer to Library", e);
		}
		synchronized(this) {
			if(lastURI == null) {
				if(badURI) return null;
				try {
					wait(1000);
				} catch (InterruptedException e) {
					// Ignore.
				}
			}
			return lastURI;
		}
	}
	
}
