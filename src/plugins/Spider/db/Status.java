/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.Spider.db;

public enum Status {
	NEW, // Newly found URIs, i.e. queued but never fetched. This puts them priority-wise before QUEUED.
	/** For simplicity, running is also mark as QUEUED. 
	 * NOT_PUSHED, when LibraryBuffer is enabled, means we have successfully fetched the page but have not
	 * yet uploaded the indexed data, so if we have an unclean shutdown we transfer all NOT_PUSHED to QUEUED
	 * so they get re-run. */
	QUEUED,
	INDEXED, // The information is sent to library.
	SUCCEEDED, // The fetch "succeeded" but we will ignore or not include the result. Also when replaced with a new edition.
	FAILED, // The fetch "failed" fatally and we will ignore the result.
	NOT_PUSHED
}