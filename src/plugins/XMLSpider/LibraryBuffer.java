
package plugins.XMLSpider;

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
import java.util.Collection;
import java.util.TreeMap;
import plugins.Library.index.TermEntryWriter;
import plugins.Library.index.TermPageEntry;

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
	private boolean enabled;

	private TreeMap<TermPageEntry, TermPageEntry> termPageBuffer = new TreeMap();

	private int bufferUsageEstimate = 0;
	private int bufferMax;

	static final File SAVE_FILE = new File("xmlspider.saved.data");

	synchronized void setBufferSize(int maxSize) {
		if(maxSize == 0 && enabled) {
			termPageBuffer.clear();
			bufferUsageEstimate = 0;
		}
		enabled = (maxSize != 0);
		bufferMax = maxSize;
	}
	
	/**
	 * Increments the estimate by specified amount and if the estimate is above the max send the buffer
	 * @param increment
	 */
	private void increaseEstimate(int increment) {
		bufferUsageEstimate += increment;
		if (bufferUsageEstimate > bufferMax)
			sendBuffer();
	}
	
	public synchronized int bufferUsageEstimate() {
		return bufferUsageEstimate;
	}
	

	LibraryBuffer(PluginRespirator pr) {
		this.pr = pr;
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
		if(!enabled) return;
		get(termPageEntry).title = s;
	}

	/**
	 * Puts a term position in the TermPageEntry and increments the bufferUsageEstimate
	 * @param tp
	 * @param position
	 */
	synchronized void addPos(TermPageEntry tp, int position) {
		if(!enabled) return;
		try{
			//Logger.normal(this, "length : "+bufferUsageEstimate+", adding to "+tp);
			get(tp).pos.put(position, "");
			//Logger.normal(this, "length : "+bufferUsageEstimate+", increasing length "+tp);
			increaseEstimate(4);
		}catch(Exception e){
			Logger.error(this, "Exception adding", e);
		}
	}


	/**
	 * Emptys the buffer into a bucket and sends it to the Library plugin with the command "pushBuffer"
	 *
	 * FIXME : I think there is something wrong with the way it writes to the bucket, I may be using the wrong kind of buffer
	 */
	private void sendBuffer() {
		if(SAVE_FILE.exists()) {
			System.out.println("Restoring data from last time from "+SAVE_FILE);
			Bucket bucket = new FileBucket(SAVE_FILE, true, false, false, false, true);
			innerSend(bucket);
			System.out.println("Restored data from last time from "+SAVE_FILE);
		}
		long tStart = System.currentTimeMillis();
		try {
			Logger.normal(this, "Sending buffer of estimated size "+bufferUsageEstimate+" bytes to Library");
			Bucket bucket = pr.getNode().clientCore.tempBucketFactory.makeBucket(3000000);
			Collection<TermPageEntry> buffer2;
			synchronized (this) {
				buffer2 = termPageBuffer.values();
				termPageBuffer = new TreeMap();
				bufferUsageEstimate = 0;
			}
			OutputStream os = bucket.getOutputStream();
			for (TermPageEntry termPageEntry : buffer2) {
				TermEntryWriter.getInstance().writeObject(termPageEntry, os);
			}
			os.close();
			bucket.setReadOnly();
			innerSend(bucket);
			Logger.normal(this, "Buffer successfully sent to Library, size = "+bucket.size());
		} catch (IOException ex) {
			Logger.error(this, "Could not make bucket to transfer buffer", ex);
		}
		long tEnd = System.currentTimeMillis();
		synchronized(this) {
			timeNotStalled += (tStart - timeLastNotStalled);
			timeLastNotStalled = tEnd;
			timeStalled += (tEnd - tStart);
		}
	}
	
	private void innerSend(Bucket bucket) {
		SimpleFieldSet sfs = new SimpleFieldSet(true);
		sfs.putSingle("command", "pushBuffer");
		PluginTalker libraryTalker;
		try {
			libraryTalker = pr.getPluginTalker(this, "plugins.Library.Main", "SpiderBuffer");
			libraryTalker.sendSyncInternalOnly(sfs, bucket);
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
		// TODO maybe
	}

	public void terminate() {
		Collection<TermPageEntry> buffer2;
		synchronized(this) {
			if(shutdown) {
				Logger.error(this, "Shutdown called twice", new Exception("error"));
				return;
			}
			shutdown = true;
			buffer2 = termPageBuffer.values();
			termPageBuffer = new TreeMap();
			bufferUsageEstimate = 0;
		}
		FileBucket bucket = new FileBucket(SAVE_FILE, false, false, false, false, false);
		OutputStream os;
		try {
			os = bucket.getOutputStream();
			for (TermPageEntry termPageEntry : buffer2) {
				TermEntryWriter.getInstance().writeObject(termPageEntry, os);
			}
			os.close();
			bucket.setReadOnly();
			System.out.println("Stored remaining data on shutdown to "+SAVE_FILE);
		} catch (IOException e) {
			// Ignore
		}
	}

}
