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
		HTMLNode configUi = configForm.addChild("ul", "class", "config");
		
		configUi.addChild("div", "class", "configprefix", "Index Writer Options");
		addConfig(configUi, //
		        "Index Directory", "Directory where the index should be written to.", // 
		        "indexDir", config.getIndexDir());
		addConfig(configUi, //
		        "Index Title", "Index Title", // 
		        "indexTitle", config.getIndexTitle());
		addConfig(configUi, //
		        "Index Owner", "Index Owner", // 
		        "indexOwner", config.getIndexOwner());
		addConfig(configUi, //
		        "Index Owner Email", "Index Owner Email", // 
		        "indexOwnerEmail", config.getIndexOwnerEmail());
		
		configForm.addChild("input", //
		        new String[] { "type", "value" }, //
		        new String[] { "submit", "Apply" });
		contentNode.addChild(configBox);
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
}