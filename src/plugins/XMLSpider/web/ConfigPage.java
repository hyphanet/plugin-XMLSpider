/**
 * Configuration page
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider.web;

import plugins.XMLSpider.Config;
import plugins.XMLSpider.XMLSpider;
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
	public void processPostRequest(HTTPRequest request, HTMLNode contentNode) {
		// Create Index
		if (request.isPartSet("testButton")) {
			HTMLNode infobox = pageMaker.getInfobox("infobox infobox-success", "Test Button Pressed!");
			infobox.addChild("#", "Test passed!");
			contentNode.addChild(infobox);
		}
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
		        "Max Parallel Requests", "Maximum number of parallel requests.", // 
		        "maxParallelRequests", //
		        new String[] { "0", "10", "50", "100", "250", "500" }, //
		        Integer.toString(config.getMaxParallelRequests()));
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