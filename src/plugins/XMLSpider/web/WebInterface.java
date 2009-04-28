/**
 * Web reuqest handlers
 * 
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider.web;

import plugins.XMLSpider.XMLSpider;
import freenet.clients.http.PageMaker;
import freenet.pluginmanager.PluginHTTPException;
import freenet.support.HTMLNode;
import freenet.support.api.HTTPRequest;

public class WebInterface {
	private final XMLSpider xmlSpider;
	private PageMaker pageMaker;

	/**
	 * @param spider
	 */
	public WebInterface(XMLSpider spider) {
		xmlSpider = spider;

		pageMaker = xmlSpider.getPageMaker();
		pageMaker.addNavigationLink("/plugins/plugins.XMLSpider.XMLSpider", //
		        "Home", "Home page", false, null);
		pageMaker.addNavigationLink("/plugins/plugins.XMLSpider.XMLSpider?ConfigPage", //
		        "Config", "Configuration", false, null);
		pageMaker.addNavigationLink("/plugins/", "Plugins page", "Back to Plugins page", false, null);
	}

	/**
	 * Interface to the Spider data
	 */
	public String handleHTTPGet(HTTPRequest request) throws PluginHTTPException {
		HTMLNode pageNode = pageMaker.getPageNode(XMLSpider.pluginName, null);
		HTMLNode contentNode = pageMaker.getContentNode(pageNode);

		WebPage page = getPageObject(request);
		page.writeContent(request, contentNode);

		return pageNode.generate();
	}

	public String handleHTTPPost(HTTPRequest request) throws PluginHTTPException {
		HTMLNode pageNode = pageMaker.getPageNode(XMLSpider.pluginName, null);
		HTMLNode contentNode = pageMaker.getContentNode(pageNode);

		WebPage page = getPageObject(request);

		page.processPostRequest(request, contentNode);
		page.writeContent(request, contentNode);

		return pageNode.generate();
	}

	public WebPage getPageObject(HTTPRequest request) {
		if (request.isParameterSet("ConfigPage"))
			return new ConfigPage(xmlSpider);
		return new MainPage(xmlSpider);
	}
}