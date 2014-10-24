/**
 * Main page
 * @author j16sdiz (1024D/75494252)
 */
package plugins.Spider.web;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import plugins.Spider.Spider;
import plugins.Spider.db.Config;
import plugins.Spider.db.Page;
import plugins.Spider.db.PerstRoot;
import plugins.Spider.db.Status;
import freenet.clients.http.InfoboxNode;
import freenet.clients.http.PageMaker;
import freenet.keys.FreenetURI;
import freenet.pluginmanager.PluginRespirator;
import freenet.support.HTMLNode;
import freenet.support.Logger;
import freenet.support.TimeUtil;
import freenet.support.api.HTTPRequest;

class MainPage implements WebPage {
	static class PageStatus {
		long count;
		List<Page> pages;

		PageStatus(long count, List<Page> pages) {
			this.count = count;
			this.pages = pages;
		}
	}

	private final Spider spider;
	private final PageMaker pageMaker;
	private final PluginRespirator pr;

	MainPage(Spider spider) {
		this.spider = spider;
		pageMaker = spider.getPageMaker();
		pr = spider.getPluginRespirator();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see plugins.Spider.WebPage#processPostRequest(freenet.support.api.HTTPRequest,
	 * freenet.support.HTMLNode)
	 */
	public void processPostRequest(HTTPRequest request, HTMLNode contentNode) {
		// Queue URI
		String addURI = request.getPartAsString("addURI", 512);
		if (addURI != null && addURI.length() != 0) {
			try {
				FreenetURI uri = new FreenetURI(addURI);
				spider.queueURI(uri, "manually", true);

				pageMaker.getInfobox("infobox infobox-success", "URI Added", contentNode).
					addChild("#", "Added " + uri);
			} catch (Exception e) {
				pageMaker.getInfobox("infobox infobox-error", "Error adding URI", contentNode).
					addChild("#", e.getMessage());
				Logger.normal(this, "Manual added URI cause exception", e);
			}
			spider.startSomeRequests();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see plugins.Spider.WebPage#writeContent(freenet.support.api.HTTPRequest,
	 * freenet.support.HTMLNode)
	 */
	public void writeContent(HTTPRequest request, HTMLNode contentNode) {
		HTMLNode overviewTable = contentNode.addChild("table", "class", "column");
		HTMLNode overviewTableRow = overviewTable.addChild("tr");

		PageStatus queuedStatus = getPageStatus(Status.QUEUED);
		PageStatus indexedStatus = getPageStatus(Status.INDEXED);
		PageStatus succeededStatus = getPageStatus(Status.SUCCEEDED);
		PageStatus failedStatus = getPageStatus(Status.FAILED);
		PageStatus notPushedStatus = getPageStatus(Status.NOT_PUSHED);

		List<Page> runningFetch = spider.getRunningFetch();
		Config config = spider.getConfig();

		// Column 1
		HTMLNode nextTableCell = overviewTableRow.addChild("td", "class", "first");
		HTMLNode statusContent = pageMaker.getInfobox("#", "Spider Status", nextTableCell);
		statusContent.addChild("#", "Running Request: " + runningFetch.size() + "/"
		        + config.getMaxParallelRequests());
		statusContent.addChild("br");
		statusContent.addChild("#", "Queued: " + queuedStatus.count);
		statusContent.addChild("br");
		statusContent.addChild("#", "Indexed: " + indexedStatus.count);
		statusContent.addChild("br");
		statusContent.addChild("#", "Succeeded: " + succeededStatus.count);
		statusContent.addChild("br");
		statusContent.addChild("#", "Not pushed: " + notPushedStatus.count);
		statusContent.addChild("br");
		statusContent.addChild("#", "Failed: " + failedStatus.count);
		statusContent.addChild("br");
		statusContent.addChild("#", "Queued Event: " + spider.callbackExecutor.getQueue().size());
		statusContent.addChild("br");
		statusContent.addChild("#", "Library buffer size: "+spider.getLibraryBufferSize());
		long lastRequestFinishedAt = spider.getLastRequestFinishedAt();
		long tStalled = spider.getStalledTime();
		long tNotStalled = spider.getNotStalledTime();
		statusContent.addChild("br");
		statusContent.addChild("#", "Last Activity: " + ((lastRequestFinishedAt == 0) ? "never" : (TimeUtil.formatTime(System.currentTimeMillis() - lastRequestFinishedAt) + " ago")));
		statusContent.addChild("br");
		statusContent.addChild("#", "Time stalled: "+TimeUtil.formatTime(tStalled));
		statusContent.addChild("br");
		statusContent.addChild("#", "Time not stalled: "+TimeUtil.formatTime(tNotStalled)+" ("+(tNotStalled * 100.0) / (tStalled + tNotStalled)+"%)");
		FreenetURI uri = spider.getURI();
		statusContent.addChild("br");
		if(uri != null && uri.getSuggestedEdition() >= 0) {
			statusContent.addChild("#", "Key: ");
			statusContent.addChild(HTMLNode.link("/"+uri.toString(false, false))).addChild("#", uri.toShortString());
		} else if(uri != null) {
			statusContent.addChild("#", "Key will be: ");
			uri = uri.setSuggestedEdition(uri.getSuggestedEdition()+1);
			statusContent.addChild(HTMLNode.link("/"+uri.toString(false, false))).addChild("#", uri.toShortString());
		} else {
			statusContent.addChild("#", "Key: Unknown");
		}
		
		// Column 2
		nextTableCell = overviewTableRow.addChild("td", "class", "second");
		HTMLNode mainContent = pageMaker.getInfobox("#", "Main", nextTableCell);
		HTMLNode addForm = pr.addFormChild(mainContent, "plugins.Spider.Spider", "addForm");
		addForm.addChild("label", "for", "addURI", "Add URI:");
		addForm.addChild("input", new String[] { "name", "style" }, new String[] { "addURI", "width: 20em;" });
		addForm.addChild("input", new String[] { "type", "value" }, new String[] { "submit", "Add" });

		InfoboxNode running = pageMaker.getInfobox("Running URI");
		HTMLNode runningBox = running.outer;
		runningBox.addAttribute("style", "right: 0;");
		HTMLNode runningContent = running.content;

		if (runningFetch.isEmpty()) {
			runningContent.addChild("#", "NO URI");
		} else {
			HTMLNode list = runningContent.addChild("ol", "style", "overflow: auto; white-space: nowrap;");

			Iterator<Page> pi = runningFetch.iterator();
			int maxURI = config.getMaxShownURIs();
			for (int i = 0; i < maxURI && pi.hasNext(); i++) {
				Page page = pi.next();
				HTMLNode litem = list.addChild("li", "title", page.getComment());
				litem.addChild("a", "href", "/freenet:" + page.getURI(), page.getURI());
			}
		}
		contentNode.addChild(runningBox);

		InfoboxNode queued = pageMaker.getInfobox("Queued URI");
		HTMLNode queuedBox = queued.outer;
		queuedBox.addAttribute("style", "right: 0; overflow: auto;");
		HTMLNode queuedContent = queued.content;
		listPages(queuedStatus, queuedContent);
		contentNode.addChild(queuedBox);

		InfoboxNode indexed = pageMaker.getInfobox("Indexed URI");
		HTMLNode indexedBox = indexed.outer;
		indexedBox.addAttribute("style", "right: 0;");
		HTMLNode indexedContent = indexed.content;
		listPages(indexedStatus, indexedContent);
		contentNode.addChild(indexedBox);

		InfoboxNode succeeded = pageMaker.getInfobox("Succeeded URI");
		HTMLNode succeededBox = succeeded.outer;
		succeededBox.addAttribute("style", "right: 0;");
		HTMLNode succeededContent = succeeded.content;
		listPages(succeededStatus, succeededContent);
		contentNode.addChild(succeededBox);

		InfoboxNode notPushed = pageMaker.getInfobox("Not pushed URI");
		HTMLNode notPushedBox = notPushed.outer;
		notPushedBox.addAttribute("style", "right: 0;");
		HTMLNode notPushedContent = notPushed.content;
		listPages(notPushedStatus, notPushedContent);
		contentNode.addChild(notPushedBox);

		InfoboxNode failed = pageMaker.getInfobox("Failed URI");
		HTMLNode failedBox = failed.outer;
		failedBox.addAttribute("style", "right: 0;");
		HTMLNode failedContent = failed.content;
		listPages(failedStatus, failedContent);
		contentNode.addChild(failedBox);
	}

	//-- Utilities
	private PageStatus getPageStatus(Status status) {
		PerstRoot root = spider.getRoot();
		synchronized (root) {
			int count = root.getPageCount(status);
			Iterator<Page> it = root.getPages(status);

			int showURI = spider.getConfig().getMaxShownURIs();
			List<Page> page = new ArrayList();
			while (page.size() < showURI && it.hasNext()) {
				page.add(it.next());
			}

			return new PageStatus(count, page);
		}
	}

	private void listPages(PageStatus pageStatus, HTMLNode parent) {
		if (pageStatus.pages.isEmpty()) {
			parent.addChild("#", "NO URI");
		} else {
			HTMLNode list = parent.addChild("ol", "style", "overflow: auto; white-space: nowrap;");

			for (Page page : pageStatus.pages) {
				HTMLNode litem = list.addChild("li", "title", page.getComment());
				litem.addChild("a", "href", "/freenet:" + page.getURI(), page.getURI());
			}
		}
	}
}
