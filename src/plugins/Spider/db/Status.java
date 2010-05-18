/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.Spider.db;

public enum Status {
	/** For simplicity, running is also mark as QUEUED. 
	 * NOT_PUSHED, when LibraryBuffer is enabled, means we have successfully fetched the page but have not
	 * yet uploaded the indexed data, so if we have an unclean shutdown we transfer all NOT_PUSHED to QUEUED
	 * so they get re-run. */
	QUEUED, INDEXED, SUCCEEDED, FAILED, NOT_PUSHED
}