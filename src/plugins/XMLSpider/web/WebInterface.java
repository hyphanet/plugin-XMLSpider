/**
 * Web reuqest handlers
 * 
 * @author j16sdiz (1024D/75494252)
 */
package plugins.XMLSpider.web;

import plugins.XMLSpider.XMLSpider;
import freenet.client.HighLevelSimpleClient;
import freenet.clients.http.PageMaker;
import freenet.clients.http.ToadletContainer;
import freenet.node.NodeClientCore;

public class WebInterface {
	private final XMLSpider xmlSpider;
	private PageMaker pageMaker;
	private ConfigPageToadlet configToadlet;
	private MainPageToadlet mainToadlet;
	private final ToadletContainer toadletContainer;
	private final HighLevelSimpleClient client;
	private final NodeClientCore core;

	/**
	 * @param spider
	 * @param client 
	 */
	public WebInterface(XMLSpider spider, HighLevelSimpleClient client, ToadletContainer container, NodeClientCore core) {
		xmlSpider = spider;

		pageMaker = xmlSpider.getPageMaker();
		this.toadletContainer = container;
		this.client = client;
		this.core = core;
	}
	
	public void load() {
		pageMaker.addNavigationCategory("/xmlspider/", "XMLSpider", "XMLSpider", xmlSpider);
		
		toadletContainer.register(mainToadlet = new MainPageToadlet(client, xmlSpider, core), "XMLSpider", "/xmlspider/", true, "XMLSpider", "XMLSpider", true, null);
		toadletContainer.register(configToadlet = new ConfigPageToadlet(client, xmlSpider, core), "XMLSpider", "/xmlspider/config", true, "Configure XMLSpider", "Configure XMLSpider", true, null);
	}

	
	public void unload() {
		toadletContainer.unregister(configToadlet);
		toadletContainer.unregister(mainToadlet);
		pageMaker.removeNavigationCategory("XMLSpider");
	}
}