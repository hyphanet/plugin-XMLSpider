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

				HTMLNode infobox = pageMaker.getInfobox("infobox infobox-success", "Scheduled Creating Index");
				infobox.addChild("#", "Index will start create soon.");
				contentNode.addChild(infobox);
			}
		}

		// Queue URI
		String addURI = request.getPartAsString("addURI", 512);
		if (addURI != null && addURI.length() != 0) {
			try {
				FreenetURI uri = new FreenetURI(addURI);
				xmlSpider.queueURI(uri, "manually", true);

				HTMLNode infobox = pageMaker.getInfobox("infobox infobox-success", "URI Added");
				infobox.addChild("#", "Added " + uri);
				contentNode.addChild(infobox);
			} catch (Exception e) {
				HTMLNode infobox = pageMaker.getInfobox("infobox infobox-error", "Error adding URI");
				infobox.addChild("#", e.getMessage());
				contentNode.addChild(infobox);
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
		HTMLNode statusBox = pageMaker.getInfobox("Spider Status");
		HTMLNode statusContent = pageMaker.getContentNode(statusBox);
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
			if (xmlSpider.isWritingIndex())
				statusContent.addChild("span", "style", "color: red; font-weight: bold;", "RUNNING");
			else if (xmlSpider.isWriteIndexScheduled())
				statusContent.addChild("span", "style", "color: blue; font-weight: bold;", "SCHEDULED");
			else
				statusContent.addChild("span", "style", "color: green; font-weight: bold;", "IDLE");
		}
		statusContent.addChild("br");
		statusContent.addChild("#", "Last Written: "
		        + (xmlSpider.getIndexWriter().tProducedIndex == 0 ? "NEVER" : new Date(
		                xmlSpider.getIndexWriter().tProducedIndex).toString()));
		nextTableCell.addChild(statusBox);

		// Column 2
		nextTableCell = overviewTableRow.addChild("td", "class", "second");
		HTMLNode mainBox = pageMaker.getInfobox("Main");
		HTMLNode mainContent = pageMaker.getContentNode(mainBox);
		HTMLNode addForm = pr.addFormChild(mainContent, "plugins.XMLSpider.XMLSpider", "addForm");
		addForm.addChild("label", "for", "addURI", "Add URI:");
		addForm.addChild("input", new String[] { "name", "style" }, new String[] { "addURI", "width: 20em;" });
		addForm.addChild("input", "type", "submit");
		nextTableCell.addChild(mainBox);

		HTMLNode indexBox = pageMaker.getInfobox("Create Index");
		HTMLNode indexContent = pageMaker.getContentNode(indexBox);
		HTMLNode indexForm = pr.addFormChild(indexContent, "plugins.XMLSpider.XMLSpider", "indexForm");
		indexForm.addChild("input", //
		        new String[] { "name", "type", "value" },//
		        new String[] { "createIndex", "hidden", "createIndex" });
		indexForm.addChild("input", //
		        new String[] { "type", "value" }, //
		        new String[] { "submit", "Create Index Now" });
		nextTableCell.addChild(indexBox);

		HTMLNode runningBox = pageMaker.getInfobox("Running URI");
		runningBox.addAttribute("style", "right: 0;");
		HTMLNode runningContent = pageMaker.getContentNode(runningBox);

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

		HTMLNode queuedBox = pageMaker.getInfobox("Queued URI");
		queuedBox.addAttribute("style", "right: 0; overflow: auto;");
		HTMLNode queuedContent = pageMaker.getContentNode(queuedBox);
		listPages(queuedStatus, queuedContent);
		contentNode.addChild(queuedBox);

		HTMLNode succeededBox = pageMaker.getInfobox("Succeeded URI");
		succeededBox.addAttribute("style", "right: 0;");
		HTMLNode succeededContent = pageMaker.getContentNode(succeededBox);
		listPages(succeededStatus, succeededContent);
		contentNode.addChild(succeededBox);

		HTMLNode failedBox = pageMaker.getInfobox("Failed URI");
		failedBox.addAttribute("style", "right: 0;");
		HTMLNode failedContent = pageMaker.getContentNode(failedBox);
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