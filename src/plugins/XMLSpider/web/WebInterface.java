/**
 * Web reuqest handlers
 * 
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider.web;

import plugins.XMLSpider.XMLSpider;
import freenet.client.HighLevelSimpleClient;
import freenet.clients.http.PageMaker;
import freenet.clients.http.PageNode;
import freenet.clients.http.Toadlet;
import freenet.clients.http.ToadletContainer;
import freenet.pluginmanager.PluginHTTPException;
import freenet.support.HTMLNode;
import freenet.support.api.HTTPRequest;

public class WebInterface {
	private final XMLSpider xmlSpider;
	private PageMaker pageMaker;
	private ConfigPageToadlet configToadlet;
	private MainPageToadlet mainToadlet;
	private final ToadletContainer toadletContainer;
	private final HighLevelSimpleClient client;

	/**
	 * @param spider
	 * @param client 
	 */
	public WebInterface(XMLSpider spider, HighLevelSimpleClient client, ToadletContainer container) {
		xmlSpider = spider;

		pageMaker = xmlSpider.getPageMaker();
		this.toadletContainer = container;
		this.client = client;
	}
	
	public void load() {
		pageMaker.addNavigationCategory("/xmlspider/", "XMLSpider", "XMLSpider", xmlSpider);
		
		toadletContainer.register(mainToadlet = new MainPageToadlet(client, xmlSpider), "XMLSpider", "/xmlspider/", true, "XMLSpider", "XMLSpider", true, null);
		toadletContainer.register(configToadlet = new ConfigPageToadlet(client, xmlSpider), "XMLSpider", "/xmlspider/config", true, "Configure XMLSpider", "Configure XMLSpider", true, null);
	}

	/**
	 * Interface to the Spider data
	 */
	public String handleHTTPGet(HTTPRequest request) throws PluginHTTPException {
		PageNode p = pageMaker.getPageNode(XMLSpider.pluginName, null);
		HTMLNode pageNode = p.outer;
		HTMLNode contentNode = p.content;

		WebPage page = getPageObject(request);
		page.writeContent(request, contentNode);

		return pageNode.generate();
	}

	public String handleHTTPPost(HTTPRequest request) throws PluginHTTPException {
		PageNode p = pageMaker.getPageNode(XMLSpider.pluginName, null);
		HTMLNode pageNode = p.outer;
		HTMLNode contentNode = p.content;

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
	
	public void unload() {
		toadletContainer.unregister(configToadlet);
		toadletContainer.unregister(mainToadlet);
		pageMaker.removeNavigationCategory("XMLSpider.category");
	}
}