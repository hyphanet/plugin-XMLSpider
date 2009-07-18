/**
 * Main page
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider.web;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import plugins.XMLSpider.XMLSpider;
import plugins.XMLSpider.db.Config;
import plugins.XMLSpider.db.Page;
import plugins.XMLSpider.db.PerstRoot;
import plugins.XMLSpider.db.Status;
import freenet.clients.http.InfoboxNode;
import freenet.clients.http.PageMaker;
import freenet.keys.FreenetURI;
import freenet.pluginmanager.PluginRespirator;
import freenet.support.HTMLNode;
import freenet.support.Logger;
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

	private final XMLSpider xmlSpider;
	private final PageMaker pageMaker;
	private final PluginRespirator pr;

	MainPage(XMLSpider xmlSpider) {
		this.xmlSpider = xmlSpider;
		pageMaker = xmlSpider.getPageMaker();
		pr = xmlSpider.getPluginRespirator();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see plugins.XMLSpider.WebPage#processPostRequest(freenet.support.api.HTTPRequest,
	 * freenet.support.HTMLNode)
	 */
	public void processPostRequest(HTTPRequest request, HTMLNode contentNode) {
		// Create Index
		if (request.isPartSet("createIndex")) {
			synchronized (this) {
				xmlSpider.scheduleMakeIndex();

				pageMaker.getInfobox("infobox infobox-success", "Scheduled Creating Index", contentNode).
					addChild("#", "Index will start create soon.");
			}
		}
		if (request.isPartSet("pausewrite")) {
			if(xmlSpider.pauseWrite())
				pageMaker.getInfobox("infobox infobox-success", "Writing task paused", contentNode)
						.addChild("#", "Schedule writing to the same directory to continue");
			else
				pageMaker.getInfobox("infobox infobox-error", "Write task could not be paused", contentNode);
		}
		if (request.isPartSet("cancelwrite")) {
			if(xmlSpider.cancelWrite())
				pageMaker.getInfobox("infobox infobox-success", "Writing task cancelled", contentNode);
			else
				pageMaker.getInfobox("infobox infobox-error", "Write task could not be cancelled, it has already started", contentNode);
		}

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
	 * @see plugins.XMLSpider.WebPage#writeContent(freenet.support.api.HTTPRequest,
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
		statusContent.addChild("#", "Index Writer: ");
		synchronized (this) {
			if (xmlSpider.isWritingIndex()){
				statusContent.addChild("span", "style", "color: red; font-weight: bold;", "RUNNING");
				HTMLNode pauseform = pr.addFormChild(statusContent, "/xmlspider/", "pauseform");
				pauseform.addChild("input", //
						new String[] { "name", "type", "value" },//
						new String[] { "pausewrite", "hidden", "pausewrite" });
				pauseform.addChild("input", new String[]{"type", "value"}, new String[]{"submit", "Pause write"});
			}else if (xmlSpider.isWriteIndexScheduled()){
				statusContent.addChild("span", "style", "color: blue; font-weight: bold;", "SCHEDULED");
				HTMLNode cancelform = pr.addFormChild(statusContent, "/xmlspider/", "cancelform");
				cancelform.addChild("input", //
						new String[] { "name", "type", "value" },//
						new String[] { "cancelwrite", "hidden", "cancelwrite" });
				cancelform.addChild("input", new String[]{"type", "value"}, new String[]{"submit", "Cancel write"});
			}else
				statusContent.addChild("span", "style", "color: green; font-weight: bold;", "IDLE");
		}
		statusContent.addChild("br");
		statusContent.addChild("#", "Last Written: "
		        + (xmlSpider.getIndexWriter().tProducedIndex == 0 ? "NEVER" : new Date(
		                xmlSpider.getIndexWriter().tProducedIndex).toString()));

		// Column 2
		nextTableCell = overviewTableRow.addChild("td", "class", "second");
		HTMLNode mainContent = pageMaker.getInfobox("#", "Main", nextTableCell);
		HTMLNode addForm = pr.addFormChild(mainContent, "plugins.XMLSpider.XMLSpider", "addForm");
		addForm.addChild("label", "for", "addURI", "Add URI:");
		addForm.addChild("input", new String[] { "name", "style" }, new String[] { "addURI", "width: 20em;" });
		addForm.addChild("input", "type", "submit");

		HTMLNode indexContent = pageMaker.getInfobox("#", "Create Index", nextTableCell);
		HTMLNode indexForm = pr.addFormChild(indexContent, "plugins.XMLSpider.XMLSpider", "indexForm");
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
			while (page.size() < showURI && it.hasNext())
				page.add(it.next());

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