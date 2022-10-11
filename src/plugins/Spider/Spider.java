/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package plugins.Spider;

import static java.lang.System.currentTimeMillis;
import static plugins.Spider.SearchUtil.isStopWord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import plugins.Spider.index.TermPageEntry;
import plugins.Spider.db.Config;
import plugins.Spider.db.Page;
import plugins.Spider.db.PerstRoot;
import plugins.Spider.db.Status;
import plugins.Spider.org.garret.perst.Storage;
import plugins.Spider.org.garret.perst.StorageFactory;
import plugins.Spider.web.WebInterface;

import freenet.client.ClientMetadata;
import freenet.client.FetchContext;
import freenet.client.FetchException;
import freenet.client.FetchResult;
import freenet.client.async.ClientContext;
import freenet.client.async.ClientGetCallback;
import freenet.client.async.ClientGetter;
import freenet.client.async.USKCallback;
import freenet.clients.http.PageMaker;
import freenet.client.filter.ContentFilter;
import freenet.client.filter.FoundURICallback;
import freenet.client.filter.UnsafeContentTypeException;
import freenet.keys.FreenetURI;
import freenet.keys.USK;
import freenet.l10n.BaseL10n.LANGUAGE;
import freenet.node.NodeClientCore;
import freenet.node.RequestClient;
import freenet.node.RequestStarter;
import freenet.pluginmanager.FredPlugin;
import freenet.pluginmanager.FredPluginL10n;
import freenet.pluginmanager.FredPluginRealVersioned;
import freenet.pluginmanager.FredPluginThreadless;
import freenet.pluginmanager.FredPluginVersioned;
import freenet.pluginmanager.PluginRespirator;
import freenet.support.Logger;
import freenet.support.api.Bucket;
import freenet.support.io.Closer;
import freenet.support.io.NativeThread;
import freenet.support.io.NullBucket;
import freenet.support.io.ResumeFailedException;

/**
 * Spider. Produces xml index for searching words. 
 * In case the size of the index grows up a specific threshold the index is split into several subindices.
 * The indexing key is the md5 hash of the word.
 * 
 *  @author swati goyal
 *  
 */
public class Spider implements FredPlugin, FredPluginThreadless,
		FredPluginVersioned, FredPluginRealVersioned, FredPluginL10n, USKCallback, RequestClient {

	/** Document ID of fetching documents */
	protected Map<Page, ClientGetter> runningFetch = Collections.synchronizedMap(new HashMap<Page, ClientGetter>());

	/**
	 * Lists the allowed mime types of the fetched page. 
	 */
	protected Set<String> allowedMIMETypes;

	static int dbVersion = 46;
	static int version = 53;

	/** We use the standard http://127.0.0.1:8888/ for parsing HTML regardless of what the local
	 * interface is actually configured to be. This will not affect the extracted FreenetURI's,
	 * and if it did it would be a security risk. */
	static final String ROOT_URI = "http://127.0.0.1:8888/";
	
	public static final String pluginName = "Spider " + version;

	public String getVersion() {
		return version + "(" + dbVersion + ") r" + Version.getSvnRevision();
	}
	
	public long getRealVersion() {
		return version;
	}

	private FetchContext ctx;
	private ClientContext clientContext;
	private boolean stopped = true;

	private NodeClientCore core;
	private PageMaker pageMaker;	
	private PluginRespirator pr;

	private LibraryBuffer librarybuffer;

	private final AtomicLong lastRequestFinishedAt = new AtomicLong();
	private final AtomicInteger subscribedToUSKs = new AtomicInteger();
	private final AtomicInteger editionsFound = new AtomicInteger();

	private Map<USK, Set<FreenetURI>> urisToReplace = Collections.synchronizedMap(new HashMap<USK, Set<FreenetURI>>());

	public int getLibraryBufferSize() {
		return librarybuffer.bufferUsageEstimate();
	}
	
	public long getStalledTime() {
		return librarybuffer.getTimeStalled();
	}
	
	public long getNotStalledTime() {
		return librarybuffer.getTimeNotStalled();
	}

	public long getLastRequestFinishedAt() {
		return lastRequestFinishedAt.get();
	}

	public int getSubscribedUSKs() {
		return subscribedToUSKs.get() - editionsFound.get();
	}

	public int getSubscribedToUSKs() {
		return subscribedToUSKs.get();
	}

	public int getEditionsFound() {
		return editionsFound.get();
	}

	public Config getConfig() {
		return getRoot().getConfig();
	}

	public boolean isGarbageCollecting() {
		return garbageCollecting;
	}

	// Set config asynchronously
	public void setConfig(Config config) {
		getRoot().setConfig(config); // hack -- may cause race condition. but this is more user friendly
		callbackExecutor.execute(new SetConfigCallback(config));
	}
	
	/**
	 * Adds the found uri to the list of to-be-retrieved uris. <p>Every usk uri added as ssk.
	 * @param uri the new uri that needs to be fetched for further indexing
	 */
	public void queueURI(FreenetURI uri, String comment, boolean force) {
		String sURI = uri.toString();
		String lowerCaseURI = sURI.toLowerCase(Locale.US);
		for (String ext : getRoot().getConfig().getBadlistedExtensions()) {
			if (lowerCaseURI.endsWith(ext)) {
				return; // be smart
			}
		}
		
		for(String keyword : getRoot().getConfig().getBadlistedKeywords()) {
			if(lowerCaseURI.indexOf(keyword.toLowerCase()) != -1) {
				return; // Fascist keyword exclusion feature. Off by default!
			}
		}

		if (uri.isUSK()) {
			FreenetURI usk = uri;
			try {
				if (uri.getSuggestedEdition() < 0) {
					uri = uri.setSuggestedEdition((-1) * uri.getSuggestedEdition());
				}
				uri = ((USK.create(uri)).getSSK()).getURI();
			} catch (MalformedURLException e) {
			}
			subscribeUSK(usk, uri);
		}

		db.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
		boolean dbTransactionEnded = false;
		try {
			Page page = getRoot().getPageByURI(uri, true, comment);
			if (force && page.getStatus() != Status.QUEUED) {
				page.setComment(comment);
				if (page.getStatus() != Status.NEW) { 
					page.setStatus(Status.QUEUED);
				}
			}

			db.endThreadTransaction();
			dbTransactionEnded = true;
		} catch (RuntimeException e) {
			Logger.error(this, "Runtime Exception: " + e, e);		
			throw e;
		} finally {
			if (!dbTransactionEnded) {
				Logger.minor(this, "rollback transaction", new Exception("debug"));
				db.rollbackThreadTransaction();
			}
		}
	}

	private void subscribeUSK(FreenetURI uri, FreenetURI uriToReplace) {
		USK usk;
		try {
			usk = USK.create(uri);
		} catch (MalformedURLException e) {
			return;
		}
		Set<FreenetURI> uris = urisToReplace.get(usk);
		if (uris == null) {
			subscribedToUSKs.getAndIncrement();
			(clientContext.uskManager).subscribe(usk, this, false, this);
			uris = new HashSet<FreenetURI>();
		}
		uris.add(uriToReplace);
		urisToReplace.put(usk, uris);
	}

	/**
	 * Start requests from the queue if less than 80% of the max requests are running until the max requests are running.
	 */
	public void startSomeRequests() {
		ArrayList<ClientGetter> toStart = null;
		synchronized (this) {
			if (stopped) return;

			int maxParallelRequests = getRoot().getConfig().getMaxParallelRequests();

			synchronized (runningFetch) {
				int running = runningFetch.size();

				if (running >= maxParallelRequests * 0.8) return;

				// Prepare to start
				toStart = new ArrayList<ClientGetter>(maxParallelRequests - running);
				db.beginThreadTransaction(Storage.COOPERATIVE_TRANSACTION);
				getRoot().sharedLockPages(Status.NEW);
				try {
					Iterator<Page> it = getRoot().getPages(Status.NEW);

					while (running + toStart.size() < maxParallelRequests && it.hasNext()) {
						Page page = it.next();
						// Skip if getting this page already
						if (runningFetch.containsKey(page)) continue;
						
						try {
							ClientGetter getter = makeGetter(page);

							Logger.minor(this, "Starting " + getter + " " + page);
							toStart.add(getter);
							runningFetch.put(page, getter);
						} catch (MalformedURLException e) {
							Logger.error(this, "IMPOSSIBLE-Malformed URI: " + page, e);
							page.setStatus(Status.FAILED);
							page.setComment("MalformedURLException");
						}
					}
				} finally {
					getRoot().unlockPages(Status.NEW);
					db.endThreadTransaction();
				}
				db.beginThreadTransaction(Storage.COOPERATIVE_TRANSACTION);
				getRoot().sharedLockPages(Status.QUEUED);
				try {
					Iterator<Page> it = getRoot().getPages(Status.QUEUED);

					while (running + toStart.size() < maxParallelRequests && it.hasNext()) {
						Page page = it.next();
						// Skip if getting this page already
						if (runningFetch.containsKey(page)) continue;
						
						try {
							ClientGetter getter = makeGetter(page);

							Logger.minor(this, "Starting " + getter + " " + page);
							toStart.add(getter);
							runningFetch.put(page, getter);
						} catch (MalformedURLException e) {
							Logger.error(this, "IMPOSSIBLE-Malformed URI: " + page, e);
							page.setStatus(Status.FAILED);
							page.setComment("MalformedURLException");
						}
					}
				} finally {
					getRoot().unlockPages(Status.QUEUED);
					db.endThreadTransaction();
				}
			}

			db.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
			getRoot().exclusiveLock(Status.INDEXED);
			try {
				Iterator<Page> it = getRoot().getPages(Status.INDEXED);
				int started = 0;
				while (started < maxParallelRequests && it.hasNext()) {
					Page page = it.next();
//					if (page.getLastChange().after(new Date().) {
//						break;
//					}
					FreenetURI uri;
					try {
						uri = new FreenetURI(page.getURI());
						if (uri.isSSKForUSK()) {
							USK usk = USK.create(uri.uskForSSK());
							if (urisToReplace.containsKey(usk)) continue;
							subscribeUSK(usk.getURI(), uri);
							page.setStatus(Status.INDEXED);
							started++;
						}
					} catch (MalformedURLException e) {
						// This could not be converted.
					}
				}
			} finally {
				getRoot().unlockPages(Status.INDEXED);
				db.endThreadTransaction();
			}
		}

		for (ClientGetter g : toStart) {
			try {
				g.start(clientContext);
				Logger.minor(this, g + " started");
			} catch (FetchException e) {
				g.getClientCallback().onFailure(e, g);
			}
		}
	}

	/**
	 * Callback for fetching the pages
	 */
	private class ClientGetterCallback implements ClientGetCallback {
		final Page page;

		public ClientGetterCallback(Page page) {
			this.page = page;
		}

        @Override
		public void onFailure(FetchException e, ClientGetter state) {
			if (stopped) return;

			callbackExecutor.execute(new OnFailureCallback(e, state, page));
			Logger.minor(this, "Queued OnFailure: " + page + " (q:" + callbackExecutor.getQueue().size() + ")");
		}

        @Override
		public void onSuccess(final FetchResult result, final ClientGetter state) {
			if (stopped) return;

			callbackExecutor.execute(new OnSuccessCallback(result, state, page));
			Logger.minor(this, "Queued OnSuccess: " + page + " (q:" + callbackExecutor.getQueue().size() + ")");
		}

		public String toString() {
			return super.toString() + ":" + page;
		}

        @Override
        public void onResume(ClientContext context) throws ResumeFailedException {
        }

        @Override
        public RequestClient getRequestClient() {
            return Spider.this;
        }
	}

	private ClientGetter makeGetter(Page page) throws MalformedURLException {
		ClientGetter getter = new ClientGetter(new ClientGetterCallback(page),
				new FreenetURI(page.getURI()), ctx,
				getPollingPriorityProgress(), null);
		return getter;
	}

	protected class OnFailureCallback implements Runnable {
		private FetchException e;
		private ClientGetter state;
		private Page page;

		OnFailureCallback(FetchException e, ClientGetter state, Page page) {
			this.e = e;
			this.state = state;
			this.page = page;
		}

		public void run() {
			onFailure(e, state, page);
		}
	}

	/**
	 * Callback for asyncronous handling of a success
	 */
	protected class OnSuccessCallback implements Runnable {
		private FetchResult result;
		private ClientGetter state;
		private Page page;

		OnSuccessCallback(FetchResult result, ClientGetter state, Page page) {
			this.result = result;
			this.state = state;
			this.page = page;
		}

		public void run() {
			onSuccess(result, state, page);
		}
	}

	/**
	 * Set config asynchronously
	 */
	protected class SetConfigCallback implements Runnable {
		private Config config;

		SetConfigCallback(Config config) {
			this.config = config;
		}

		public void run() {
			synchronized (getRoot()) {
				getRoot().setConfig(config);
				startSomeRequests();
			}
		}
	}

	protected class StartSomeRequestsCallback implements Runnable {
		StartSomeRequestsCallback() {
		}

		public void run() {
			try {
				Thread.sleep(30000);
			} catch (InterruptedException e) {
				// ignore
			}
			startSomeRequests();
		}
	}

	protected static class CallbackPrioritizer implements Comparator<Runnable> {
		public int compare(Runnable o1, Runnable o2) {
			if (o1.getClass() == o2.getClass()) return 0;

			return getPriority(o1) - getPriority(o2);
		}

		private int getPriority(Runnable r) {
			if (r instanceof SetConfigCallback) {
				return 0;
			} else if (r instanceof OnFailureCallback) {
				return 2;
			} else if (r instanceof OnSuccessCallback) {
				return 3;
			} else if (r instanceof StartSomeRequestsCallback) {
				return 4;
			}

			return -1;
		}
	}

	// this is java.util.concurrent.Executor, not freenet.support.Executor
	// always run with one thread --> more thread cause contention and slower!
	public ThreadPoolExecutor callbackExecutor = new ThreadPoolExecutor( //
			1, 1, 600, TimeUnit.SECONDS, //
	        new PriorityBlockingQueue<Runnable>(5, new CallbackPrioritizer()), //
	        new ThreadFactory() {
		        public Thread newThread(Runnable r) {
			        Thread t = new NativeThread(r, "Spider", NativeThread.NORM_PRIORITY - 1, true);
			        t.setDaemon(true);
			        t.setContextClassLoader(Spider.this.getClass().getClassLoader());
			        return t;
		        }
	        });

	/**
	 * Processes the successfully fetched uri for further outlinks.
	 * 
	 * @param result
	 * @param state
	 * @param page
	 */
	// single threaded
	protected void onSuccess(FetchResult result, ClientGetter state, Page page) {
		synchronized (this) {
			if (stopped) return;    				
		}

		lastRequestFinishedAt.set(currentTimeMillis());
		FreenetURI uri = state.getURI();
		ClientMetadata cm = result.getMetadata();
		Bucket data = result.asBucket();
		String mimeType = cm.getMIMEType();

		boolean dbTransactionEnded = false;
		db.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
		try {
			librarybuffer.setBufferSize(getConfig().getNewFormatIndexBufferLimit()*1024*1024);
			/*
			 * instead of passing the current object, the pagecallback object for every page is
			 * passed to the content filter this has many benefits to efficiency, and allows us to
			 * identify trivially which page is being indexed. (we CANNOT rely on the base href
			 * provided).
			 */
			PageCallBack pageCallBack = new PageCallBack(page);
			Logger.minor(this, "Successful: " + uri + " : " + page.getId());

			try {
				if ("text/plain".equals(mimeType)) {
					try (BufferedReader br = new BufferedReader(new InputStreamReader(data.getInputStream()))) {
						String line;
						while ((line = br.readLine()) != null) {
							pageCallBack.onText(line, mimeType, uri.toURI(ROOT_URI));
						}
					}
				} else {
					try (InputStream filterInput = data.getInputStream();
						 OutputStream filterOutput = new NullBucket().getOutputStream()) {
						ContentFilter.filter(filterInput, filterOutput, mimeType, uri.toURI(ROOT_URI), pageCallBack,
								null, null);
					}
				}
				pageCallBack.finish();
				librarybuffer.maybeSend();

			} catch (UnsafeContentTypeException e) {
				// wrong mime type
				page.setStatus(Status.SUCCEEDED);
				page.setComment("UnsafeContentTypeException");
				db.endThreadTransaction();
				dbTransactionEnded = true;

				Logger.minor(this, "UnsafeContentTypeException " + uri + " : " + page.getId(), e);
				return; // Ignore
			} catch (IOException e) {
				// ugh?
				Logger.error(this, "Bucket error?: " + e, e);
				return;
			} catch (Exception e) {
				// we have lots of invalid html on net - just normal, not error
				Logger.normal(this, "exception on content filter for " + page, e);
				return;
			}

			page.setStatus(Status.NOT_PUSHED);
			db.endThreadTransaction();
			dbTransactionEnded  = true;

			Logger.minor(this, "Filtered " + uri + " : " + page.getId());
		} catch (RuntimeException e) {
			// other runtime exceptions
			Logger.error(this, "Runtime Exception: " + e, e);		
			throw e;
		} finally {
			try {
				data.free();

				synchronized (this) {
					runningFetch.remove(page);
				}
				if (!stopped) startSomeRequests();
			} finally {
				if (!dbTransactionEnded) {
					Logger.minor(this, "rollback transaction", new Exception("debug"));
					db.rollbackThreadTransaction();
					db.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
					// page is now invalidated.
					page = getRoot().getPageByURI(uri, false, "");
					if(page != null) {
						page.setStatus(Status.FAILED);
						page.setComment("could not complete operation dbTransaction not ended");
					}
					db.endThreadTransaction();
				}
			}
		}
	}

	protected void onFailure(FetchException fe, ClientGetter getter, Page page) {
		Logger.minor(this, "Failed: " + page + " : " + getter, fe);

		synchronized (this) {
			if (stopped) return;
		}

		lastRequestFinishedAt.set(currentTimeMillis());
		boolean dbTransactionEnded = false;
		db.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
		try {
			synchronized (page) {
				if (fe.newURI != null) {
					// redirect, mark as succeeded
					queueURI(fe.newURI, "redirect from " + getter.getURI(), false);
					page.setStatus(Status.SUCCEEDED);
					page.setComment("Redirected");
				} else if (fe.isFatal()) {
					// too many tries or fatal, mark as failed
					page.setStatus(Status.FAILED);
					page.setComment("Fatal");
				} else {
					// requeue at back
					page.setStatus(Status.QUEUED);
				}
			}
			db.endThreadTransaction();
			dbTransactionEnded = true;
		} catch (Exception e) {
			Logger.error(this, "Unexcepected exception in onFailure(): " + e, e);
			throw new RuntimeException("Unexcepected exception in onFailure()", e);
		} finally {
			runningFetch.remove(page);
			if (!dbTransactionEnded) {
				Logger.minor(this, "rollback transaction", new Exception("debug"));
				db.rollbackThreadTransaction();
			}
		}

		startSomeRequests();
	} 

	private boolean garbageCollecting = false;

	/**
	 * Stop the plugin, pausing any writing which is happening
	 */
	public void terminate(){
		Logger.normal(this, "Spider terminating");

		synchronized (this) {
			stopped = true;

			for (Map.Entry<Page, ClientGetter> me : runningFetch.entrySet()) {
				ClientGetter getter = me.getValue();
				Logger.minor(this, "Canceling request" + getter);
				getter.cancel(clientContext);
			}
			runningFetch.clear();
			callbackExecutor.shutdownNow();
		}
		librarybuffer.terminate();

		try { callbackExecutor.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException e) {}
		try { db.close(); } catch (Exception e) {}
		
		webInterface.unload();
		
		Logger.normal(this, "Spider terminated");
	}

	/**
	 * Start plugin
	 * @param pr
	 */
	public synchronized void runPlugin(PluginRespirator pr) {
		this.core = pr.getNode().clientCore;
		this.pr = pr;
		pageMaker = pr.getPageMaker();

		/* Initialize Fetch Context */
		this.ctx = pr.getHLSimpleClient().getFetchContext();
		ctx.maxSplitfileBlockRetries = 1;
		ctx.maxNonSplitfileRetries = 1;
		ctx.maxTempLength = 2 * 1024 * 1024;
		ctx.maxOutputLength = 2 * 1024 * 1024;
		allowedMIMETypes = new HashSet<String>();
		allowedMIMETypes.add("text/html");
		allowedMIMETypes.add("text/plain");
		allowedMIMETypes.add("application/xhtml+xml");
		ctx.allowedMIMETypes = new HashSet<String>(allowedMIMETypes);
		clientContext = pr.getNode().clientCore.clientContext;

		stopped = false;

		// Initial Database
		db = initDB();

		webInterface = new WebInterface(this, pr.getHLSimpleClient(), pr.getToadletContainer(), pr.getNode().clientCore);
		webInterface.load();

		FreenetURI[] initialURIs = core.getBookmarkURIs();
		for (int i = 0; i < initialURIs.length; i++) {
			queueURI(initialURIs[i], "bookmark", false);
		}

		librarybuffer = new LibraryBuffer(pr, this);
		librarybuffer.start();

		callbackExecutor.execute(new StartSomeRequestsCallback());
	}

	private WebInterface webInterface;

	/**
	 * creates the callback object for each page.
	 *<p>Used to create inlinks and outlinks for each page separately.
	 * @author swati
	 *
	 */
	public class PageCallBack implements FoundURICallback{
		protected final Page page;
		private FreenetURI uri;
		private String title;
		private int totalWords;

		protected final boolean logDEBUG = Logger.shouldLog(Logger.DEBUG, this); // per instance, allow changing on the fly

		PageCallBack(Page page) {
			this.page = page;
			try {
				this.uri = new FreenetURI(page.getURI());
			} catch (MalformedURLException ex) {
				Logger.error(this, "Error creating uri from '"+page.getURI()+"'", ex);
			}
			//Logger.normal(this, "Parsing "+page.getURI());
		}

		@Override
		public void foundURI(FreenetURI uri) {
			// Ignore
		}

		/**
		 * When a link is found in html
		 * @param uri
		 * @param inline
		 */
		@Override
		public void foundURI(FreenetURI uri, boolean inline) {
			if (stopped) throw new RuntimeException("plugin stopping");
			if (logDEBUG) Logger.debug(this, "foundURI " + uri + " on " + page);
			queueURI(uri, "Added from " + page.getURI(), false);
		}

		protected Integer lastPosition = null;

		/**
		 * When text is found
		 * @param s
		 * @param type
		 * @param baseURI
		 */
		@Override
		public void onText(String s, String type, URI baseURI){
			if (stopped) throw new RuntimeException("plugin stopping");
			if (logDEBUG) Logger.debug(this, "onText on " + page.getId() + " (" + baseURI + ")");

			if ("title".equalsIgnoreCase(type) && (s != null) && (s.length() != 0) && (s.indexOf('\n') < 0)) {
				/*
				 * title of the page 
				 */
				page.setPageTitle(s);
				title = s;
				type = "title";
			} else {
				type = null;
			}
			// Tokenise. Do not use the pairs-of-CJK-chars option because we need
			// accurate word index numbers.
			SearchTokenizer tok = new SearchTokenizer(s, false);

			if(lastPosition == null) lastPosition = 1; 
			int i = 0;
			for (String word: tok) {
				totalWords++;
				try {
					if(type == null)
						addWord(word, lastPosition + i);
					else
						addWord(word, Integer.MIN_VALUE + i); // Put title words in the right order starting at Min_Value
				} catch (Exception e) {
					// If a word fails continue
					Logger.error(this, "Word '" + word + "' failed: "+e, e);
				} 
				i++;
			}

			if (type == null) {
				lastPosition = lastPosition + i;
			}
		}
		
		void finish() {
			for (TermPageEntry termPageEntry : tpes.values()) {
				if(title != null)
					librarybuffer.setTitle(termPageEntry, title);
				// Crude first approximation to relevance calculation.
				// Client should multiply by log ( total count of files / count of files with this word in )
				// Which is equal to log ( total count of files ) - log ( count of files with this word in )
				librarybuffer.setRelevance(termPageEntry, ((float)termPageEntry.positionsSize()) / ((float)totalWords));
			}
		}

		HashMap<String, TermPageEntry> tpes = new HashMap();

		/**
		 * Add a word to the database for this page
		 * @param word
		 * @param position
		 * @throws java.lang.Exception
		 */
		private void addWord(String word, int position) {
			if (logDEBUG) Logger.debug(this, "addWord on " + page.getId() + " (" + word + "," + position + ")");

			// Skip word if it is a stop word
			if (isStopWord(word)) return;

			// Add to Library buffer
			TermPageEntry tp = getEntry(word);
			librarybuffer.addPos(tp, position);
		}

		/**
		 * Makes a TermPageEntry for this term
		 * @param word
		 * @return
		 */
		private TermPageEntry getEntry(String word) {
			TermPageEntry tp = tpes.get(word);
			if (tp == null) {
				tp = new TermPageEntry(word, 0, uri, null);
				tpes.put(word, tp);
			}
			return tp;
		}

		@Override
		public void onFinishedPage() {
			// Ignore
		}
	}

    @Override
	public void onFoundEdition(long l, USK key, ClientContext context, boolean metadata,
            short codec, byte[] data, boolean newKnownGood, boolean newSlotToo) {
		if (newKnownGood) {
			Logger.minor(this, "Known Good. Found new Edition for " + key + ".");
			Set<FreenetURI> uris = urisToReplace.remove(key);
			if (uris != null) {
				for (FreenetURI uri : uris) {
					Page page = getRoot().getPageByURI(uri, false, "");
					if (page != null) {
						page.setComment("Replaced by new edition " + key);
						page.setStatus(Status.SUCCEEDED);
					}
				}
			}
			FreenetURI uri = key.getURI();
			queueURI(uri, "USK found edition", true);
			startSomeRequests();
			editionsFound.getAndIncrement();
		} else {
			Logger.minor(this, "Not Known Good. Edition search continues for " + key + ".");
		}
	}

    @Override
	public short getPollingPriorityNormal() {
		return (short) Math.min(RequestStarter.MINIMUM_FETCHABLE_PRIORITY_CLASS, getRoot().getConfig().getRequestPriority() + 1);
	}

    @Override
	public short getPollingPriorityProgress() {
		return getRoot().getConfig().getRequestPriority();
	}

	protected Storage db;

	/**
	 * Initializes Database
	 */
	private Storage initDB() {
		Storage db = StorageFactory.getInstance().createStorage();
		db.setProperty("perst.object.cache.kind", "pinned");
		db.setProperty("perst.object.cache.init.size", 65536); //Increasing from 8192 no longer brings my system to it's knees after a few hours.  Does this make sense?
		db.setProperty("perst.alternative.btree", true);
		db.setProperty("perst.string.encoding", "UTF-8");
		db.setProperty("perst.concurrent.iterator", true);

		db.open("Spider-" + dbVersion + ".dbs");

		PerstRoot root = (PerstRoot) db.getRoot();
		if (root == null) PerstRoot.createRoot(db);
		else {
			// Not working:
			// PerstRoot.patchRoot(db);
		}

		return db;
	}

	public PerstRoot getRoot() {
		return (PerstRoot) db.getRoot();
	}

	protected Page getPageById(long id) {
		return getRoot().getPageById(id);
	}

	// language for I10N
	private LANGUAGE language;

    @Override
	public String getString(String key) {
		// TODO return a translated string
		return key;
	}

    @Override
	public void setLanguage(LANGUAGE newLanguage) {
		language = newLanguage;
	}

	public PageMaker getPageMaker() {
		return pageMaker;
	}

	public List<Page> getRunningFetch() {
		synchronized (runningFetch) {
			return new ArrayList<Page>(runningFetch.keySet());
		}
	}

	public PluginRespirator getPluginRespirator() {
		return pr;
	}

    @Override
	public boolean persistent() {
		return false;
	}

	public void resetPages(Status from, Status to) {
		int count = 0;
		Iterator<Page> pages = getRoot().getPages(from);
		while(pages.hasNext()) {
			pages.next().setStatus(to);
			count++;
		}
		System.out.println("Reset "+count+" pages status from "+from+" to "+to);
	}

	public boolean realTimeFlag() {
		return false; // We definitely want throughput here.
	}

	public FreenetURI getURI() {
		return librarybuffer.getURI();
	}
}
