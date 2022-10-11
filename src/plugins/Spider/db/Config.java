/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.Spider.db;

import plugins.Spider.org.garret.perst.Persistent;
import plugins.Spider.org.garret.perst.Storage;
import freenet.node.RequestStarter;
import freenet.support.Logger;

import java.util.Calendar;

public class Config extends Persistent implements Cloneable {

	public static final Status[] statusesToProcess = {Status.NEW, Status.NEW_EDITION, Status.FAILED};
	public static final boolean[] workingRelationsToProcess = {true, false};
	private String indexTitle;
	private String indexOwner;
	private String indexOwnerEmail;

	private int maxShownURIs;
	/** working, status
	 * 
	 * This should be an array with dimensions working and status.
	 * This is problematic in the database so it will instead be stored
	 * as a string of semicolon-separated ints.
	 */
	private String maxParallelRequests;
	private int beginWorkingPeriod; // Between 0 and 23
	private int endWorkingPeriod; // Between 0 and 23
	private String[] badlistedExtensions;
	private String[] badlistedKeywords;
	private short requestPriority;

	private boolean debug;
	
	private int newFormatIndexBufferLimit;

	public Config() {
	}

	public Config(Storage storage) {

		indexTitle = "Spider index";
		indexOwner = "Freenet";
		indexOwnerEmail = "(nil)";

		maxShownURIs = 50;

		maxParallelRequests = "";
		beginWorkingPeriod = 23;
		endWorkingPeriod = 7;

		badlistedExtensions = new String[] { //
				".ico", ".bmp", ".png", ".jpg", ".jpeg", ".gif", ".tif", ".tiff", ".ani", ".raw", ".svg", // image
				".zip", ".jar", ".gz", ".bz2", ".rar", ".sit", // archive
		        ".7z", ".tar", ".arj", ".rpm", ".deb", //
		        ".xpi", ".ace", ".cab", ".lza", ".lzh", //
		        ".exe", ".iso", ".bin", ".dll", ".nrg", ".dmg", ".drv", ".img", ".msi", ".nds", ".vcd", // binary
		        ".mpg", ".ogg", ".ogv", ".mp3", ".avi", ".wv", ".swf", ".wmv", ".mkv", ".flac", ".ogm", ".divx", ".mpeg", ".rm", ".wma", ".asf", ".rmvb", ".mov", ".flv", ".mp4", ".m4v", ".wav", ".aac", ".cda", ".fla", ".m4a", ".midi", ".vob", // media
		        ".css", ".sig", ".gml", ".df", ".cbr", ".gf", ".pdf", ".db", ".dbf", ".accdb", ".dat", ".docx", ".dwg", ".mdf", ".odg", ".odt", ".ods", ".pps", ".wdb", ".xls", ".xlsx" // other
		};
		
		badlistedKeywords = new String[] {}; // No excluded keywords by default.

		requestPriority = RequestStarter.IMMEDIATE_SPLITFILE_PRIORITY_CLASS;
		
		newFormatIndexBufferLimit = 4;

		storage.makePersistent(this);
	}

	public synchronized Config clone() {
		try {
			Config config = (Config) super.clone();
			assert !config.isPersistent();
			return config;
		} catch (CloneNotSupportedException e) {
			Logger.error(this, "Impossible exception", e);
			throw new RuntimeException(e);
		}
	}

	public synchronized void setIndexTitle(String indexTitle) {
		assert !isPersistent();
		this.indexTitle = indexTitle;
	}

	public synchronized String getIndexTitle() {
		return indexTitle;
	}

	public synchronized void setIndexOwner(String indexOwner) {
		assert !isPersistent();
		this.indexOwner = indexOwner;
	}

	public synchronized String getIndexOwner() {
		return indexOwner;
	}

	public synchronized void setIndexOwnerEmail(String indexOwnerEmail) {
		assert !isPersistent();
		this.indexOwnerEmail = indexOwnerEmail;
	}

	public synchronized void setMaxShownURIs(int maxShownURIs) {
		assert !isPersistent();
		this.maxShownURIs = maxShownURIs;
	}

	public synchronized int getMaxShownURIs() {
		return maxShownURIs;
	}

	public synchronized String getIndexOwnerEmail() {
		return indexOwnerEmail;
	}

	private int workingIndex(boolean b) {
		for (int i = 0; i < workingRelationsToProcess.length; i++) {
			if (workingRelationsToProcess[i] == b) {
				return i;
			}
		}
		throw new RuntimeException();
	}

	private int statusIndex(Status status) {
		for (int i = 0; i < statusesToProcess.length; i++) {
			if (statusesToProcess[i] == status) {
				return i;
			}
		}
		throw new RuntimeException();
	}

	private int[][] unpackMaxParallelRequests() {
		int[][] requests = new int[workingRelationsToProcess.length][statusesToProcess.length];
		String[] arr = maxParallelRequests.split(";");
		int arrIndex = 0;
		for (int w = 0; w < workingRelationsToProcess.length; w++) {
			for (int s= 0; s < statusesToProcess.length; s++) {
				requests[w][s] = 0;
				if (arrIndex < arr.length && !arr[arrIndex].equals("")) {
					try {
						requests[w][s] = Integer.parseInt(arr[arrIndex++]);
					} catch (NumberFormatException e) {
						// Ignore if we can't do the conversion.
					}
				}
			}
		}
		return requests;
	}

	public synchronized void setMaxParallelRequests(boolean working, Status status, int maxParallelRequests) {
		assert !isPersistent();
		int[][] requests = unpackMaxParallelRequests();
		requests[workingIndex(working)][statusIndex(status)] = maxParallelRequests;
		
		StringBuilder sb = new StringBuilder();
		for (int w = 0; w < workingRelationsToProcess.length; w++) {
			for (int s= 0; s < statusesToProcess.length; s++) {
				sb.append(Integer.toString(requests[w][s]));
				sb.append(";");
			}
		}
		this.maxParallelRequests = sb.toString();
	}

	public synchronized int getMaxParallelRequests(boolean working, Status status) {
		int[][] requests = unpackMaxParallelRequests();
		return requests[workingIndex(working)][statusIndex(status)];
	}

	public synchronized int getMaxParallelRequests(Status status) {
		int actualHour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
		Boolean isWorking = true;

		if(this.getBeginWorkingPeriod() < this.getEndWorkingPeriod()) {
			// Midnight isn't in the interval.
			//        m            M
			// 0 -----|############|----- 24
			isWorking = (actualHour > this.getBeginWorkingPeriod() && actualHour < this.getEndWorkingPeriod());
		} else {
			// Midnight is in the interval.
			//        M            m
			// 0 #####|------------|##### 24
			isWorking = (actualHour > this.getBeginWorkingPeriod() || actualHour < this.getEndWorkingPeriod());
		}

		return this.getMaxParallelRequests(isWorking, status);
	}

	public synchronized void setBeginWorkingPeriod(int beginWorkingPeriod) {
		assert !isPersistent();
		this.beginWorkingPeriod = beginWorkingPeriod;
	}

	public synchronized int getBeginWorkingPeriod() {
		return beginWorkingPeriod;
	}

	public synchronized void setEndWorkingPeriod(int endWorkingPeriod) {
		assert !isPersistent();
		this.endWorkingPeriod = endWorkingPeriod;
	}

	public synchronized int getEndWorkingPeriod() {
		return endWorkingPeriod;
	}

	public synchronized void setBadlistedExtensions(String[] badlistedExtensions) {
		assert !isPersistent();
		this.badlistedExtensions = badlistedExtensions;
	}
	
	public synchronized void setBadlistedKeywords(String[] badlistedKeywords) {
		this.badlistedKeywords = badlistedKeywords;
	}
	
	public synchronized String[] getBadlistedKeywords() {
		if(badlistedKeywords == null) return new String[0];
		// FIXME remove - caused by config errors some time before 91346024428393592fc971bbcdec5f7ca5b4f055
		if(badlistedKeywords.length == 1 && badlistedKeywords[0].equals(""))
			badlistedKeywords = new String[0];
		return badlistedKeywords;
	}

	public synchronized String[] getBadlistedExtensions() {
		return badlistedExtensions;
	}

	public synchronized void setRequestPriority(short requestPriority) {
		assert !isPersistent();
		this.requestPriority = requestPriority;
	}

	public synchronized short getRequestPriority() {
		return requestPriority;
	}

	public synchronized boolean isDebug() {
		return debug;
	}

	public synchronized void debug(boolean debug) {
		assert !isPersistent();
		this.debug = debug;
	}
	
	public synchronized int getNewFormatIndexBufferLimit() {
		return newFormatIndexBufferLimit;
	}
	
	public synchronized void setNewFormatIndexBufferLimit(int limit) {
		newFormatIndexBufferLimit = limit;
	}
}
