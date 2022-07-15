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
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
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
import freenet.support.Logger.LogLevel;
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
		FredPluginVersioned, FredPluginRealVersioned, FredPluginL10n, RequestClient {

	/** Document ID of fetching documents */
	protected Map<Status, Map<FreenetURI, ClientGetter>> runningFetches = new HashMap<Status, Map<FreenetURI, ClientGetter>>();
	{
		for (Status status : Config.statusesToProcess) {
			runningFetches.put(status, Collections.synchronizedMap(new HashMap<FreenetURI, ClientGetter>()));
		}
	}

	private Map<ClientGetter, ScheduledFuture<?>> runningFutures = Collections.synchronizedMap(new HashMap<ClientGetter, ScheduledFuture<?>>());

	/**
	 * Lists the allowed mime types of the fetched page. 
	 */
	protected Set<String> allowedMIMETypes;

	static int dbVersion = 48;
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
	private final AtomicInteger editionsFound = new AtomicInteger();

	private final Set<SubscribedToUSK> subscribedToUSKs = Collections.synchronizedSet(new HashSet<SubscribedToUSK>());

	private Map<Status, BulkPageIterator> bulkPageIterators = null;

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

	public int getSubscribedToUSKs() {
		return subscribedToUSKs.size();
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
	 * Adds the found uri to the list of to-be-retrieved uris. <p>
	 *
	 * SSKs are added as their corresponding USK.
	 *
	 * Uris already in the database are not added. New Uris are put in NEW.
	 *
	 * USKs in the database but with new edition are moved to NEW_EDITION.
	 *
	 * @param uri the new uri that needs to be fetched for further indexing
	 */
	public void queueURI(FreenetURI uri, String comment) {
		String sURI = uri.toString();
		final long NO_USK = -1L;
		long edition = NO_USK;
		String lowerCaseURI = sURI.toLowerCase(Locale.US);
		for (String ext : getRoot().getConfig().getBadlistedExtensions()) {
			if (lowerCaseURI.endsWith(ext)) {
				return; // be smart, don't fetch certain files
			}
		}
		
		for(String keyword : getRoot().getConfig().getBadlistedKeywords()) {
			if(lowerCaseURI.indexOf(keyword.toLowerCase()) != -1) {
				return; // Fascist keyword exclusion feature. Off by default!
			}
		}

		// Always add an USK page without the '-' to trigger search of versions.
		if (uri.isUSK() && uri.getSuggestedEdition() < 0) {
			uri = uri.setSuggestedEdition(-uri.getSuggestedEdition());
		}

		// Never add an SSK if there could be a corresponding USK
		if (uri.isSSKForUSK()) {
			uri = uri.uskForSSK();
		}

		if (uri.isUSK()) {
			edition = uri.getSuggestedEdition();
			uri = uri.setSuggestedEdition(0L);
		}

		db.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
		boolean dbTransactionEnded = false;
		try {
			Page page = getRoot().getPageByURI(uri, true, comment);
			if (edition != NO_USK) {
				final long oldEdition = page.getEdition();
				if (edition > oldEdition) {
					Status whereTo = Status.NEW_EDITION;
					if (!page.hasBeenFetched()) {
						whereTo = Status.NEW;
					}
					page.setStatus(edition, whereTo, "New edition replacing " + oldEdition);
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

	private class SubscribedToUSK implements USKCallback {
		private static final int DELAY_IN_MINUTES_AFTER_NEW_EDITION_SEEN = 10;
		private FreenetURI uri;
		USK usk;

		SubscribedToUSK(FreenetURI theURI) {
			uri = theURI;
			try {
				usk = USK.create(uri);
			} catch (MalformedURLException e) {
				Logger.error(this, "Cannot subscribe to " + uri + ".", e);
				return;
			}
			(clientContext.uskManager).subscribe(usk, this, false, Spider.this);
		}

		@Override
		public void onFoundEdition(long l, final USK key, ClientContext context, boolean metadata,
				short codec, byte[] data, boolean newKnownGood, boolean newSlot) {
			Logger.minor(this, "Found new Edition for " + key + ", newKnownGood=" + newKnownGood + " newSlot=" + newSlot + ".");
			editionsFound.getAndIncrement();
			final FreenetURI uri = key.getURI();

			callbackExecutor.schedule(new Runnable() {
				@Override
				public void run() {
					Logger.debug(this, "Queueing new Edition for " + key + ".");
					queueURI(uri, "USK found edition " + uri);
				}
			}, DELAY_IN_MINUTES_AFTER_NEW_EDITION_SEEN, TimeUnit.MINUTES);
		}

		public void unsubscribe() {
			(clientContext.uskManager).unsubscribe(usk, this);
			subscribedToUSKs.remove(this);
		}

		@Override
		public short getPollingPriorityNormal() {
			return (short) Math.min(RequestStarter.MINIMUM_FETCHABLE_PRIORITY_CLASS, getRoot().getConfig().getRequestPriority() + 1);
		}

		@Override
		public short getPollingPriorityProgress() {
			return getRoot().getConfig().getRequestPriority();
		}
	}

	private void subscribeUSK(FreenetURI uri) {
		subscribedToUSKs.add(new SubscribedToUSK(uri));
	}

	/**
	 * Fetches pages from a queue, many at the time to avoid locking
	 * the database on every fetch.
	 */
	class BulkPageIterator implements Iterator<Page> {
		private Status queue;
		private Deque<Page> list = new LinkedList<Page>();
		private int BULK_FETCH_SIZE = 1000;
		private long TIME_TO_DEFER_DATABASE_READ = TimeUnit.SECONDS.toMillis(30);
		private Date lastPoll = new Date();

		BulkPageIterator(Status status) {
			queue = status;
		}

		/**
		 * Fills the cache with pages.
		 * If the consumer went through the cache to quickly, don't
		 * fill it, emulating an empty iterator. This addresses the
		 * case when all the found pages are in progress. It also sets
		 * a cap on the amount of pages processed per time unit.
		 * @param extraFetches is amount of pages to fetch on top of 
		 * 						BULK_FETCH_SIZE.
		 */
		private void fill(int extraFetches) {
			Date now = new Date();
			if (list.isEmpty() && now.after(new Date(lastPoll.getTime() + TIME_TO_DEFER_DATABASE_READ))) {
				lastPoll = now;
				db.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
				try {
					Iterator<Page> it = getRoot().getPages(queue);
					int i = 0;
					while (it.hasNext()) {
						list.offer(it.next());
						if (++i > BULK_FETCH_SIZE + extraFetches) {
							break;
						}
					}
				} finally {
					db.endThreadTransaction();
				}
			}
		}

		public boolean hasNext(int extraFetches) {
			fill(extraFetches);
			return !list.isEmpty();
		}

		@Override
		public boolean hasNext() {
			fill(0);
			return !list.isEmpty();
		}

		@Override
		public Page next() {
			return list.poll();
		}
	}

	/**
	 * Start requests from new and queued.
	 */
	private void startFetches() {
		synchronized (this) {
			if (stopped) {
				bulkPageIterators = null;
				return;
			}

			if (bulkPageIterators == null) {
				bulkPageIterators = new HashMap<Status, BulkPageIterator>();
			}
		}

		for (Status status : Config.statusesToProcess) {
			ArrayList<ClientGetter> toStart = null;
			synchronized (this) {
				Map<FreenetURI, ClientGetter> runningFetch = runningFetches.get(status);
				synchronized (runningFetch) {
					int maxParallelRequests = getRoot().getConfig().getMaxParallelRequests(status);
					int running = runningFetch.size();

					if (maxParallelRequests <= running) {
						continue;
					}

					toStart = new ArrayList<ClientGetter>(maxParallelRequests - running);

					if (!bulkPageIterators.containsKey(status)) {
						bulkPageIterators.put(status, new BulkPageIterator(status));
					}

					BulkPageIterator bulkPageIterator = bulkPageIterators.get(status);
					while (running + toStart.size() < maxParallelRequests &&
							bulkPageIterator.hasNext(maxParallelRequests)) {
						Page page = bulkPageIterator.next();
						Logger.debug(this, "Page " + page + " found in " + status + ".");
						// Skip if getting this page already
						if (runningFetch.containsKey(page.getURI())) continue;

						try {
							FreenetURI uri = new FreenetURI(page.getURI());
							if (uri.isUSK()) {
								uri = uri.setSuggestedEdition(page.getEdition());
							}
							ClientGetter getter = makeGetter(uri);

							Logger.minor(this, "Starting new " + getter + " " + page);
							toStart.add(getter);
							runningFetch.put(uri, getter);
						} catch (MalformedURLException e) {
							Logger.error(this, "IMPOSSIBLE-Malformed URI: " + page, e);
							page.setStatus(Status.FATALLY_FAILED, "MalformedURLException");
						}
					}
				}
			}

			for (final ClientGetter g : toStart) {
				try {
					g.start(clientContext);
					Logger.minor(this, g + " started");
				} catch (FetchException e) {
					g.getClientCallback().onFailure(e, g);
					continue;
				}
				ScheduledFuture<?> future = callbackExecutor.scheduleWithFixedDelay(new Runnable() {
					long lapsLeft = 10 * 60 * 60; // Ten hours
					@Override
					public void run() {
						if (lapsLeft-- <= 0) {
							g.cancel(clientContext);
							Logger.minor(this, g + " aborted because of time-out");
							ScheduledFuture<?> f = runningFutures.get(g);
							f.cancel(false);
						}
					}
				}, 10, 1, TimeUnit.SECONDS);
				runningFutures.put(g, future);
			}
		}
	}

	/**
	 * Subscribe to USKs for PROCESSED_USKs.
	 */
	private void subscribeAllUSKs() {
		synchronized (this) {
			if (stopped) return;

			db.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
			try {
				Iterator<Page> it = getRoot().getPages(Status.PROCESSED_USK);
				while (it.hasNext()) {
					Page page = it.next();
					Logger.debug(this, "Page " + page + " found in PROCESSED_USK.");
					FreenetURI uri; 
					try {
						uri = new FreenetURI(page.getURI());
					} catch (MalformedURLException e) {
						// This could not be converted - ignore.
						Logger.error(this, "USK could not be converted to uri " + page);
						page.setStatus(Status.FATALLY_FAILED);
						continue;
					}
					if (uri.isUSK()) {
						subscribeUSK(uri);
					} else {
						Logger.error(this, "USK was not USK " + page);
						page.setStatus(Status.FATALLY_FAILED);
					}
				}
			} finally {
				db.endThreadTransaction();
			}
		}
	}

	/**
	 * Callback for fetching the pages
	 */
	private class ClientGetterCallback implements ClientGetCallback {
		@Override
		public void onFailure(FetchException e, ClientGetter state) {
			Logger.minor(this,
				     "onFailure: " + state.getURI() + " (q:" + callbackExecutor.getQueue().size() + ")",
				     e);
			removeFuture(state);

			if (stopped) return;

			callbackExecutor.execute(new OnFailureCallback(e, state));
		}

		@Override
		public void onSuccess(final FetchResult result, final ClientGetter state) {
			Logger.minor(this, "onSuccess: " + state.getURI() + " (q:" + callbackExecutor.getQueue().size() + ")");
			removeFuture(state);

			if (stopped) return;

			callbackExecutor.execute(new OnSuccessCallback(result, state));
		}

		private void removeFuture(ClientGetter getter) {
			ScheduledFuture<?> future = runningFutures.remove(getter);
			if (future != null) {
				future.cancel(false);
			}
		}

        @Override
        public void onResume(ClientContext context) throws ResumeFailedException {
        }

        @Override
        public RequestClient getRequestClient() {
            return Spider.this;
        }
	}

	private ClientGetter makeGetter(FreenetURI uri) throws MalformedURLException {
		ClientGetter getter = new ClientGetter(new ClientGetterCallback(),
				uri, ctx,
				getPollingPriorityProgress(), null);
		return getter;
	}

	protected class OnFailureCallback implements Runnable {
		private FetchException e;
		private ClientGetter state;

		OnFailureCallback(FetchException e, ClientGetter state) {
			this.e = e;
			this.state = state;
		}

		public void run() {
			onFailure(e, state);
		}
	}

	/**
	 * Callback for asyncronous handling of a success
	 */
	protected class OnSuccessCallback implements Runnable {
		private FetchResult result;
		private ClientGetter state;

		OnSuccessCallback(FetchResult result, ClientGetter state) {
			this.result = result;
			this.state = state;
		}

		public void run() {
			onSuccess(result, state);
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
			}
		}
	}

	// this is java.util.concurrent.Executor, not freenet.support.Executor
	// always run with one thread --> more thread cause contention and slower!
	public ScheduledThreadPoolExecutor callbackExecutor = new ScheduledThreadPoolExecutor(
			1,
	        new ThreadFactory() {
		        public Thread newThread(Runnable r) {
			        Thread t = new NativeThread(r, "Spider", NativeThread.PriorityLevel.NORM_PRIORITY.value - 1, true);
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
	 */
	// single threaded
	protected void onSuccess(FetchResult result, ClientGetter state) {
		synchronized (this) {
			if (stopped) return;    				
		}

		FreenetURI uri = state.getURI();
		FreenetURI dbURI = uri;
		if (uri.isUSK() ) {
			dbURI = uri.setSuggestedEdition(0L);
		}

		lastRequestFinishedAt.set(currentTimeMillis());
		ClientMetadata cm = result.getMetadata();
		Bucket data = result.asBucket();
		String mimeType = cm.getMIMEType();

		boolean dbTransactionEnded = false;
		db.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
		Page page = null;
		try {
			page = getRoot().getPageByURI(dbURI);
			if (page == null) {
				Logger.error(this, "Cannot find page " + dbURI);
				return;
			}
			librarybuffer.setBufferSize(1 + getConfig().getNewFormatIndexBufferLimit()*1024*1024);
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
				page.setStatus(Status.NOT_PUSHED);
				page.setLastFetched();
				librarybuffer.maybeSend();

			} catch (UnsafeContentTypeException e) {
				// wrong mime type
				page.setStatus(Status.FATALLY_FAILED, "UnsafeContentTypeException");
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
				Logger.normal(this, "exception on content filter for " + uri, e);
				return;
			}

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
					removeFromRunningFetches(uri);
				}
			} finally {
				if (!dbTransactionEnded) {
					Logger.minor(this, "rollback transaction", new Exception("debug"));
					db.rollbackThreadTransaction();
					db.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
					// page is now invalidated.
					if (page != null) {
						page.setStatus(Status.FATALLY_FAILED,
								"could not complete operation dbTransaction not ended");
					}
					db.endThreadTransaction();
				}
			}
		}
	}

	private void removeFromRunningFetches(FreenetURI uri) {
		if (runningFetches != null) {
			for (Status status : Config.statusesToProcess) {
				if (runningFetches.containsKey(status)) {
					if (runningFetches.get(status).remove(uri) != null) {
						break;
					}
				}
			}
		}
	}

	/**
	 * Do what needs to be done when a fetch request has failed.
	 *
	 * @param fe Is the exception that make it fail.
	 *           Used to decide what to do.
	 * @param getter is the ClientGetter that failed.
	 */
	protected void onFailure(FetchException fe, ClientGetter getter) {
		final FreenetURI uri = getter.getURI();
		Logger.minor(this, "Failed: " + uri + " : " + getter);

		synchronized (this) {
			if (stopped) return;
		}

		FreenetURI dbURI = uri;
		if (uri.isUSK() ) {
			dbURI = uri.setSuggestedEdition(0L);
		}

		lastRequestFinishedAt.set(currentTimeMillis());
		boolean dbTransactionEnded = false;
		db.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
		Page page = null;
		try {
			page = getRoot().getPageByURI(dbURI);
			if (page == null) {
				return;
			}
			if (fe.newURI != null) {
				// mark as succeeded
				Status whereTo = Status.DONE;
				if (uri.isUSK()) {
					whereTo = Status.PROCESSED_USK;
				} else if (uri.isKSK()) {
					whereTo = Status.PROCESSED_KSK;
				}
				FreenetURI newURI = fe.newURI;
				if (fe.mode == FetchException.FetchExceptionMode.NOT_ENOUGH_PATH_COMPONENTS) {
					if (uri.isUSK() && !uri.hasMetaStrings()) {
						newURI = uri.pushMetaString("");
						whereTo = Status.DONE;
					}
				}
				page.setStatus(whereTo, "Redirected to " + newURI + " because of " + fe.getMode());
				// redirect. This is done in an independent Runnable to get its own lock. 
				final FreenetURI redirectedTo = newURI;
				final FreenetURI redirectedFrom = uri;
				callbackExecutor.execute(new Runnable() {
					@Override
					public void run() {
						// If this is a new Edition it is moved again from PROCESSED_USK to NEW_EDITION.
						queueURI(redirectedTo, "redirect from " + redirectedFrom);
					}
					
				});
			} else if (fe.isFatal()) {
				// too many tries or fatal, mark as failed
				page.setStatus(Status.FATALLY_FAILED, "Fatal: " + fe.getMode());
			} else {
				// requeue at back
				page.setStatus(Status.FAILED);
			}
			db.endThreadTransaction();
			dbTransactionEnded = true;
		} catch (Exception e) {
			Logger.error(this, "Unexcepected exception in onFailure(): " + e, e);
			throw new RuntimeException("Unexcepected exception in onFailure()", e);
		} finally {
			removeFromRunningFetches(uri);
			if (!dbTransactionEnded) {
				Logger.minor(this, "rollback transaction", new Exception("debug"));
				db.rollbackThreadTransaction();
			}
		}
	} 

	private boolean garbageCollecting = false;

	/**
	 * Stop the plugin, pausing any writing which is happening
	 */
	public void terminate(){
		Logger.normal(this, "Spider terminating");

		synchronized (this) {
			stopped = true;

			for (Status status : Config.statusesToProcess) {
				for (Map.Entry<FreenetURI, ClientGetter> me : runningFetches.get(status).entrySet()) {
					ClientGetter getter = me.getValue();
					Logger.minor(this, "Canceling request" + getter);
					getter.cancel(clientContext);
				}
				runningFetches.get(status).clear();
			}
			for (SubscribedToUSK stu : new HashSet<SubscribedToUSK>(subscribedToUSKs)) {
				stu.unsubscribe();
			}
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
			queueURI(initialURIs[i], "bookmark");
		}

		librarybuffer = new LibraryBuffer(pr, this);
		librarybuffer.start();

		callbackExecutor.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				try {
					startFetches();
				} catch (Throwable e) {
					Logger.error(this, "startFetches throws", e);
				}
			}
			
		}, 30L, 1L, TimeUnit.SECONDS);
		callbackExecutor.schedule(new Runnable() {
			@Override
			public void run() {
				try {
					subscribeAllUSKs();
				} catch (Throwable e) {
					Logger.error(this, "startSubscribeUSKs throws", e);
				}
			}
			
		}, 10L, TimeUnit.SECONDS);
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

		protected final boolean logDEBUG = Logger.shouldLog(LogLevel.DEBUG, this); // per instance, allow changing on the fly

		PageCallBack(Page page) {
			this.page = page;
			try {
				uri = new FreenetURI(page.getURI());
				final long edition = page.getEdition();
				if (edition != 0L) {
					uri = uri.setSuggestedEdition(edition);
				}
				Logger.debug(this, "New PageCallBack for " + this.page + " (" + this.uri + ").");
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
			queueURI(uri, "Added from " + this.uri);
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
			Logger.debug(this, "Finished PageCallBack for " + this.page + " (" + this.uri + ").");
		}

		HashMap<String, TermPageEntry> tpes = new HashMap<String, TermPageEntry>();

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

		return db;
	}

	public PerstRoot getRoot() {
		return (PerstRoot) db.getRoot();
	}

	protected Page getPageById(long id) {
		return getRoot().getPageById(id);
	}


    @Override
	public String getString(String key) {
		// TODO return a translated string
		return key;
	}

    @Override
	public void setLanguage(LANGUAGE newLanguage) {
		Logger.debug(this, "New language set " + newLanguage + " - ignored.");
	}

	public PageMaker getPageMaker() {
		return pageMaker;
	}

	public List<String> getRunningFetch(Status status) {
		List<String> result = new ArrayList<String>();
		synchronized (runningFetches) {
			if (runningFetches != null) {
				for (FreenetURI uri : runningFetches.get(status).keySet()) {
					result.add(uri.toString());
				}
			}
		}
		return result;
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
			Page page = pages.next();
			Logger.debug(this, "Page " + page + " found in " + from + ".");
			page.setStatus(to);
			count++;
		}
		System.out.println("Reset "+count+" pages status from "+from+" to "+to);
	}

	public void donePages() {
		// Not a separate transaction, commit with the index updates.
		Status from = Status.NOT_PUSHED;
		int count = 0;
		Iterator<Page> pages = getRoot().getPages(from);
		while(pages.hasNext()) {
			Page page = pages.next();
			Status to;
			FreenetURI uri;
			try {
				uri = new FreenetURI(page.getURI());
				if (uri.isCHK()) {
					to = Status.DONE;
				} else if (uri.isKSK()) {
					to = Status.PROCESSED_KSK;
				} else if (uri.isSSK()) {
					to = Status.DONE;
				} else if (uri.isUSK() &&
						uri.hasMetaStrings() && !uri.getMetaString().equals("")) {
					// This is not the top element of this USK.
					to = Status.DONE;
				} else if (uri.isUSK()) {
					to = Status.PROCESSED_USK;
					subscribeUSK(uri);
				} else {
					Logger.error(this, "Cannot understand the type of the key " + uri);
					to = Status.DONE;
				}
			} catch (MalformedURLException e) {
				to = Status.DONE;
			}
			Logger.debug(this, "Page " + page + " found in " + from + ".");
			page.setStatus(to);
			count++;
		}
		Logger.minor(this, "Considered " + count + " pages processed.");
	}

	public boolean realTimeFlag() {
		return false; // We definitely want throughput here.
	}

	public FreenetURI getURI() {
		return librarybuffer.getURI();
	}
}
