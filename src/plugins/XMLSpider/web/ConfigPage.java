/**
 * Configuration page
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider.web;

import plugins.XMLSpider.XMLSpider;
import freenet.clients.http.PageMaker;
import freenet.pluginmanager.PluginRespirator;
import freenet.support.HTMLNode;
import freenet.support.api.HTTPRequest;

class ConfigPage implements WebPage {

	private final XMLSpider xmlSpider;
	private final PageMaker pageMaker;
	private final PluginRespirator pr;

	ConfigPage(XMLSpider xmlSpider) {
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
		HTMLNode indexBox = pageMaker.getInfobox("Test");
		HTMLNode indexContent = pageMaker.getContentNode(indexBox);
		HTMLNode indexForm = pr.addFormChild(indexContent, "plugins.XMLSpider.XMLSpider?ConfigPage", "indexForm");
		indexForm.addChild("input", //
		        new String[] { "name", "type", "value" },//
		        new String[] { "testButton", "hidden", "testButton" });
		indexForm.addChild("input", //
		        new String[] { "type", "value" }, //
		        new String[] { "submit", "Button" });
		contentNode.addChild(indexBox);
	}
}