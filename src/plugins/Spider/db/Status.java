/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.Spider.db;

/**
 * This enum also control the layout of the database so
 * when changing this, be sure to update the dbVersion in
 * Spider.
 */
public enum Status {
	NEW, // Newly found URIs, i.e. never fetched.
	NOT_PUSHED,
	/**
	 * NOT_PUSHED, when LibraryBuffer is enabled, means we have successfully fetched the page but have not
	 * yet uploaded the indexed data, so if we have an unclean shutdown we transfer all NOT_PUSHED to NEW
	 * so they get re-run. */
	DONE, // The information is sent to library or there was no result. There is no more work to do.
	PROCESSED_KSK, // The KSK has been sent to the library. We will rescan this later.
	PROCESSED_USK, // The USK has been sent to the library. We will rescan this later.
	FAILED,
	FATALLY_FAILED, // The fetch "failed" fatally and we will ignore the result and never try again.
}