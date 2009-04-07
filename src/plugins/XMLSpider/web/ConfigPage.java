/**
 * Configuration page
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider.web;

import plugins.XMLSpider.XMLSpider;
import plugins.XMLSpider.db.Config;
import freenet.clients.http.PageMaker;
import freenet.pluginmanager.PluginRespirator;
import freenet.support.HTMLNode;
import freenet.support.api.HTTPRequest;

class ConfigPage implements WebPage {
	private final XMLSpider xmlSpider;
	private final PageMaker pageMaker;
	private final PluginRespirator pr;
	private Config config;

	ConfigPage(XMLSpider xmlSpider) {
		this.xmlSpider = xmlSpider;
		pageMaker = xmlSpider.getPageMaker();
		pr = xmlSpider.getPluginRespirator();

		config = xmlSpider.getConfig();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see plugins.XMLSpider.WebPage#processPostRequest(freenet.support.api.HTTPRequest,
	 * freenet.support.HTMLNode)
	 */
	public synchronized void processPostRequest(HTTPRequest request, HTMLNode contentNode) {
		config = xmlSpider.getConfig().clone();
		
		if (request.isPartSet("maxParallelRequestsWorking")) {
			int v = request.getIntPart("maxParallelRequestsWorking", config.getMaxParallelRequestsWorking());
			config.setMaxParallelRequestsWorking(v);
		}
		if (request.isPartSet("maxParallelRequestsNonWorking")) {
			int v = request.getIntPart("maxParallelRequestsNonWorking", config.getMaxParallelRequestsNonWorking());
			config.setMaxParallelRequestsNonWorking(v);
		}
		if (request.isPartSet("beginWorkingPeriod")) {
			int v = request.getIntPart("beginWorkingPeriod", config.getBeginWorkingPeriod());
			config.setBeginWorkingPeriod(v);
		}
		if (request.isPartSet("endWorkingPeriod")) {
			int v = request.getIntPart("endWorkingPeriod", config.getEndWorkingPeriod());
			config.setEndWorkingPeriod(v);
		}
		if (request.isPartSet("indexMaxEntries")) {
			int v = request.getIntPart("indexMaxEntries", config.getIndexMaxEntries());
			config.setIndexMaxEntries(v);
		}
		if (request.isPartSet("indexSubindexMaxSize")) {
			int v = request.getIntPart("indexSubindexMaxSize", config.getIndexSubindexMaxSize() / 1024 / 1024);
			config.setIndexSubindexMaxSize(v * 1024 * 1024);
		}
		if (request.isPartSet("badListedExtensions")) {
			String v = request.getPartAsString("badListedExtensions", 512);
			String[] v0 = v.split(",");
			boolean good = true;
			for (int i = 0; i < v0.length; i++) {
				v0[i] = v0[i].trim();
				if (v0[i].length() == 0 || v0[i].charAt(0) != '.') {
					good = false;
					break;
				}
			}
			if (good)
				config.setBadlistedExtensions(v0);
		}
		
		if (request.isPartSet("indexDir")) {
			String v = request.getPartAsString("indexDir", 256);
			config.setIndexDir(v);
		}
		if (request.isPartSet("indexTitle")) {
			String v = request.getPartAsString("indexTitle", 256);
			config.setIndexTitle(v);
		}
		if (request.isPartSet("indexOwner")) {
			String v = request.getPartAsString("indexOwner", 256);
			config.setIndexOwner(v);
		}
		if (request.isPartSet("indexOwnerEmail")) {
			String v = request.getPartAsString("indexOwnerEmail", 256);
			config.setIndexOwnerEmail(v);
		}
		if (request.isPartSet("debug")) {
			String v = request.getPartAsString("debug", 10);
			config.debug(Boolean.valueOf(v));
		}
		
		xmlSpider.setConfig(config);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see plugins.XMLSpider.WebPage#writeContent(freenet.support.api.HTTPRequest,
	 * freenet.support.HTMLNode)
	 */
	public void writeContent(HTTPRequest request, HTMLNode contentNode) {	
		HTMLNode configBox = pageMaker.getInfobox("Configuration");
		HTMLNode configContent = pageMaker.getContentNode(configBox);
		HTMLNode configForm = pr.addFormChild(configContent, "plugins.XMLSpider.XMLSpider?ConfigPage", "configForm");
	
		configForm.addChild("div", "class", "configprefix", "Spider Options");
		
		HTMLNode spiderConfig = configForm.addChild("ul", "class", "config");
		addConfig(spiderConfig, //
		        "Max Parallel Requests (Working)", "Maximum number of parallel requests if we are in the working period.", //
		        "maxParallelRequestsWorking", //
		        new String[] { "0", "10", "50", "100", "250", "500" }, //
		        Integer.toString(config.getMaxParallelRequestsWorking()));
		addConfig(spiderConfig, //
		        "Max Parallel Requests (Non-Working)", "Maximum number of parallel requests if we are not in the working period.", //
		        "maxParallelRequestsNonWorking", //
		        new String[] { "0", "10", "50", "100", "250", "500" }, //
		        Integer.toString(config.getMaxParallelRequestsNonWorking()));

		addConfig(spiderConfig, //
		        "Working period beginning hour", "Beginning hour of the Working period.", //
		        "beginWorkingPeriod", //
		        new String[] { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11",
			"12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23" }, //
		        Integer.toString(config.getBeginWorkingPeriod()));
		addConfig(spiderConfig, //
		        "Working period ending hour", "Ending hour of the Working period.", //
		        "endWorkingPeriod", //
		        new String[] { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11",
			"12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23" }, //
		        Integer.toString(config.getEndWorkingPeriod()));

		addConfig(spiderConfig, //
		        "Bad Listed Extensions", "Comma seprated list of banned URI suffix.", // 
		        "badListedExtensions", //
		        config.getBadlistedExtensions());
		addHTML(spiderConfig, "Blar Blar Blar", new HTMLNode("a", "href", "?Blarla", "bla"));
		
		configForm.addChild("div", "class", "configprefix", "Index Writer Options");
		
		HTMLNode indexConfig = configForm.addChild("ul", "class", "config");
		addConfig(indexConfig, //
		        "Index Directory", "Directory where the index should be written to.", // 
		        "indexDir", config.getIndexDir());
		addConfig(indexConfig, //
		        "Index Title", "Index Title", // 
		        "indexTitle", config.getIndexTitle());
		addConfig(indexConfig, //
		        "Index Owner", "Index Owner", // 
		        "indexOwner", config.getIndexOwner());
		addConfig(indexConfig, //
		        "Index Owner Email", "Index Owner Email", // 
		        "indexOwnerEmail", config.getIndexOwnerEmail());
		addConfig(indexConfig, //
		        "Maximum Subindex Entries", "Maximum number of entries in each index.", // 
		        "indexMaxEntries", //
		        new String[] { "500", "1000", "2000", "4000", "8000" }, //
		        Integer.toString(config.getIndexMaxEntries()));
		addConfig(indexConfig, //
		        "Maximum Subindex Size", "Maximum size of a subindex (MiB).", // 
		        "indexSubindexMaxSize", //
		        new String[] { "1", "2", "4", "8" }, //
		        Integer.toString(config.getIndexSubindexMaxSize() / 1024 / 1024));
		addConfig(indexConfig, //
		        "Write debug info", "Write debug info", // 
		        "debug", //
		        new String[] { "false", "true" }, //
		        Boolean.toString(config.isDebug()));
		
		configForm.addChild("input", //
		        new String[] { "type", "value" }, //
		        new String[] { "submit", "Apply" });
		contentNode.addChild(configBox);
	}

	
	private void addHTML(HTMLNode configUi, String shortDesc, HTMLNode node) {
		HTMLNode li = configUi.addChild("li");
		li.addChild("span", "class", "configshortdesc", shortDesc);
		li.addChild("span", "class", "config").addChild(node);
	}
	
	private void addConfig(HTMLNode configUi, String shortDesc, String longDesc, String name, String value) {
		HTMLNode li = configUi.addChild("li");
		li.addChild("span","class","configshortdesc", shortDesc);
		li.addChild("span","class","config") //
			.addChild("input", //
		                new String[] { "class", "type", "name", "value" }, //
		                new String[] { "config", "text", name, value });
		li.addChild("span", "class", "configlongdesc", longDesc);
	}
	
	private void addConfig(HTMLNode configUi, String shortDesc, String longDesc, String name, String[] values,
	        String value) {
		HTMLNode li = configUi.addChild("li");
		li.addChild("span","class","configshortdesc", shortDesc);
		HTMLNode select = li.addChild("span", "class", "config") //
		        .addChild("select", //
		                new String[] { "class", "name" }, //
		                new String[] { "config", name });
		for (String v : values) {
			HTMLNode o = select.addChild("option", "value", v, v);
			if (v.equals(value))
				o.addAttribute("selected", "selected");
		}
		li.addChild("span", "class", "configlongdesc", longDesc);
	}
	
	private void addConfig(HTMLNode configUi, String shortDesc, String longDesc, String name, String[] value) {
		StringBuilder value2 = new StringBuilder(value[0]);
		for (int i = 1; i < value.length; i++) {
			value2.append(", ");
			value2.append(value[i]);
		}

		HTMLNode li = configUi.addChild("li");
		li.addChild("span", "class", "configshortdesc", shortDesc);
		li.addChild("span", "class", "config") //
		        .addChild("input", //
		                new String[] { "class", "type", "name", "value" }, //
		                new String[] { "config", "text", name, value2.toString() });
		li.addChild("span", "class", "configlongdesc", longDesc);
	}
}
