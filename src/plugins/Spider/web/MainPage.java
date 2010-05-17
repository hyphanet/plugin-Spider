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

	private final Spider xmlSpider;
	private final PageMaker pageMaker;
	private final PluginRespirator pr;

	MainPage(Spider xmlSpider) {
		this.xmlSpider = xmlSpider;
		pageMaker = xmlSpider.getPageMaker();
		pr = xmlSpider.getPluginRespirator();
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
				xmlSpider.queueURI(uri, "manually", true);

				pageMaker.getInfobox("infobox infobox-success", "URI Added", contentNode).
					addChild("#", "Added " + uri);
			} catch (Exception e) {
				pageMaker.getInfobox("infobox infobox-error", "Error adding URI", contentNode).
					addChild("#", e.getMessage());
				Logger.normal(this, "Manual added URI cause exception", e);
			}
			xmlSpider.startSomeRequests();
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
		PageStatus succeededStatus = getPageStatus(Status.SUCCEEDED);
		PageStatus failedStatus = getPageStatus(Status.FAILED);

		List<Page> runningFetch = xmlSpider.getRunningFetch();
		Config config = xmlSpider.getConfig();

		// Column 1
		HTMLNode nextTableCell = overviewTableRow.addChild("td", "class", "first");
		HTMLNode statusContent = pageMaker.getInfobox("#", "Spider Status", nextTableCell);
		statusContent.addChild("#", "Running Request: " + runningFetch.size() + "/"
		        + config.getMaxParallelRequests());
		statusContent.addChild("br");
		statusContent.addChild("#", "Queued: " + queuedStatus.count);
		statusContent.addChild("br");
		statusContent.addChild("#", "Succeeded: " + succeededStatus.count);
		statusContent.addChild("br");
		statusContent.addChild("#", "Failed: " + failedStatus.count);
		statusContent.addChild("br");
		statusContent.addChild("br");
		statusContent.addChild("#", "Queued Event: " + xmlSpider.callbackExecutor.getQueue().size());
		statusContent.addChild("br");
		statusContent.addChild("br");
		statusContent.addChild("#", "Library buffer size: "+xmlSpider.getLibraryBufferSize());
		long tStalled = xmlSpider.getStalledTime();
		long tNotStalled = xmlSpider.getNotStalledTime();
		statusContent.addChild("br");
		statusContent.addChild("#", "Time stalled: "+TimeUtil.formatTime(tStalled));
		statusContent.addChild("br");
		statusContent.addChild("#", "Time not stalled: "+TimeUtil.formatTime(tNotStalled)+" ("+(tNotStalled * 100.0) / (tStalled + tNotStalled)+"%)");
		
		// Column 2
		nextTableCell = overviewTableRow.addChild("td", "class", "second");
		HTMLNode mainContent = pageMaker.getInfobox("#", "Main", nextTableCell);
		HTMLNode addForm = pr.addFormChild(mainContent, "plugins.Spider.Spider", "addForm");
		addForm.addChild("label", "for", "addURI", "Add URI:");
		addForm.addChild("input", new String[] { "name", "style" }, new String[] { "addURI", "width: 20em;" });
		addForm.addChild("input", "type", "submit");

		HTMLNode indexContent = pageMaker.getInfobox("#", "Create Index", nextTableCell);
		HTMLNode indexForm = pr.addFormChild(indexContent, "plugins.Spider.Spider", "indexForm");
		indexForm.addChild("input", //
		        new String[] { "name", "type", "value" },//
		        new String[] { "createIndex", "hidden", "createIndex" });
		indexForm.addChild("input", //
		        new String[] { "type", "value" }, //
		        new String[] { "submit", "Create Index Now" });

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

		InfoboxNode succeeded = pageMaker.getInfobox("Succeeded URI");
		HTMLNode succeededBox = succeeded.outer;
		succeededBox.addAttribute("style", "right: 0;");
		HTMLNode succeededContent = succeeded.content;
		listPages(succeededStatus, succeededContent);
		contentNode.addChild(succeededBox);

		InfoboxNode failed = pageMaker.getInfobox("Failed URI");
		HTMLNode failedBox = failed.outer;
		failedBox.addAttribute("style", "right: 0;");
		HTMLNode failedContent = failed.content;
		listPages(failedStatus, failedContent);
		contentNode.addChild(failedBox);
	}

	//-- Utilities
	private PageStatus getPageStatus(Status status) {
		PerstRoot root = xmlSpider.getRoot();
		synchronized (root) {
			int count = root.getPageCount(status);
			Iterator<Page> it = root.getPages(status);

			int showURI = xmlSpider.getConfig().getMaxShownURIs();
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
