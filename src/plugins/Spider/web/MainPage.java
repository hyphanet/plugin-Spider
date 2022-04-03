/**
 * Main page
 * @author j16sdiz (1024D/75494252)
 */
package plugins.Spider.web;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.naming.SizeLimitExceededException;

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
		String addURI = null;
		try {
			addURI = request.getPartAsStringThrowing("addURI", 512);
		} catch (SizeLimitExceededException e1) {
		} catch (NoSuchElementException e1) {
		}
		if (addURI != null && addURI.length() != 0) {
			try {
				FreenetURI uri = new FreenetURI(addURI);
				spider.queueURI(uri, "manually");

				pageMaker.getInfobox("infobox infobox-success", "URI Added", contentNode).
					addChild("#", "Added " + uri);
			} catch (Exception e) {
				pageMaker.getInfobox("infobox infobox-error", "Error adding URI", contentNode).
					addChild("#", e.getMessage());
				Logger.normal(this, "Manual added URI cause exception", e);
			}
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

		List<String> runningFetch = spider.getRunningFetch();
		Config config = spider.getConfig();

		// Column 1
		HTMLNode nextTableCell = overviewTableRow.addChild("td", "class", "first");
		HTMLNode statusContent = pageMaker.getInfobox("#", "Spider Status", nextTableCell);
		statusContent.addChild("#", "Running Request: " + runningFetch.size() + "/"
		        + config.getMaxParallelRequests());
		statusContent.addChild("br");
		for (Status status : Status.values()) {
			statusContent.addChild("#", status + ": " + getPageStatus(status).count);
			statusContent.addChild("br");
		}
		statusContent.addChild("#", "Queued Event: " + spider.callbackExecutor.getQueue().size());
		statusContent.addChild("br");
		statusContent.addChild("#", "Subscribed USKs: " + spider.getSubscribedToUSKs());
		statusContent.addChild("br");
		statusContent.addChild("#", "URIs replaced by new USKs: " + spider.getNewUSKs());
		statusContent.addChild("br");
		statusContent.addChild("#", "Found editions: " + spider.getEditionsFound());
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
		mainContent.addChild("p");
		final File file = new File(".", "library.info");
		FileReader fr = null;
		BufferedReader br = null;
		try {
			fr = new FileReader(file);
			br = new BufferedReader(fr);
			String line;
			while ((line = br.readLine()) != null) {
				mainContent.addChild("#", line);
				mainContent.addChild("br");
			}
			br.close();
		} catch (FileNotFoundException e) {
			// There is no such file. That is fine.
		} catch (IOException e) {
			// We suddenly couldn't read this file. Strange problem.
			throw new RuntimeException(e);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					// Ignore.
				}
			}
			if (fr != null) {
				try {
					fr.close();
				} catch (IOException e) {
					// Ignore.
				}
			} 
		}

		InfoboxNode running = pageMaker.getInfobox("Running URI");
		HTMLNode runningBox = running.outer;
		runningBox.addAttribute("style", "right: 0;");
		HTMLNode runningContent = running.content;

		if (runningFetch.isEmpty()) {
			runningContent.addChild("#", "NO URI");
		} else {
			runningContent.addChild("#", "USKs shown without edition.");
			HTMLNode list = runningContent.addChild("ol", "style", "overflow: auto; white-space: nowrap;");

			Iterator<String> pi = runningFetch.iterator();
			int maxURI = config.getMaxShownURIs();
			for (int i = 0; i < maxURI && pi.hasNext(); i++) {
				String runningURI = pi.next();
				HTMLNode litem = list.addChild("li");
				litem.addChild("a", "href", "/freenet:" + runningURI, runningURI);
			}
		}
		contentNode.addChild(runningBox);

		for (Status status : Status.values()) {
			InfoboxNode d = pageMaker.getInfobox(status + " URIs");
			HTMLNode box = d.outer;
			box.addAttribute("style", "right: 0; overflow: auto;");
			HTMLNode content = d.content;
			listPages(getPageStatus(status), content);
			contentNode.addChild(box);
		}
	}

	//-- Utilities
	private PageStatus getPageStatus(Status status) {
		PerstRoot root = spider.getRoot();
		synchronized (root) {
			int count = root.getPageCount(status);
			Iterator<Page> it = root.getPages(status);

			int showURI = spider.getConfig().getMaxShownURIs();
			List<Page> pages = new ArrayList<Page>();
			while (pages.size() < showURI && it.hasNext()) {
				pages.add(it.next());
			}

			return new PageStatus(count, pages);
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
				String title = page.getPageTitle();
				if (title == null) {
					title = "";
				}
				long edition = page.getEdition();
				if (edition != 0L) {
					title = "Edition " + edition + " " + title;
				}
				litem.addChild("p",
						" " +
						page.getLastChangeAsString() + " " +
						title + " " +
						"(" + page.getComment() + ")");
			}
		}
	}
}
