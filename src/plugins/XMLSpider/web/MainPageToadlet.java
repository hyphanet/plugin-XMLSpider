package plugins.XMLSpider.web;

import java.io.IOException;
import java.net.URI;

import plugins.XMLSpider.XMLSpider;

import freenet.client.HighLevelSimpleClient;
import freenet.clients.http.PageNode;
import freenet.clients.http.RedirectException;
import freenet.clients.http.Toadlet;
import freenet.clients.http.ToadletContext;
import freenet.clients.http.ToadletContextClosedException;
import freenet.support.HTMLNode;
import freenet.support.api.HTTPRequest;

public class MainPageToadlet extends Toadlet {

	final XMLSpider spider;
	
	protected MainPageToadlet(HighLevelSimpleClient client, XMLSpider spider) {
		super(client);
		this.spider = spider;
	}

	@Override
	public String path() {
		return "/xmlspider/";
	}

	@Override
	public String supportedMethods() {
		return "GET, POST";
	}

	@Override
	public void handleGet(URI uri, final HTTPRequest request, final ToadletContext ctx) 
	throws ToadletContextClosedException, IOException, RedirectException {
		ClassLoader origClassLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(XMLSpider.class.getClassLoader());
		try {
			MainPage page = new MainPage(spider);
			PageNode p = ctx.getPageMaker().getPageNode(XMLSpider.pluginName, ctx);
			HTMLNode pageNode = p.outer;
			HTMLNode contentNode = p.content;
			page.writeContent(request, contentNode);
			writeHTMLReply(ctx, 200, "OK", null, pageNode.generate());
		} finally {
			Thread.currentThread().setContextClassLoader(origClassLoader);
		}
	}
	
	@Override
	public void handlePost(URI uri, HTTPRequest request, final ToadletContext ctx) throws ToadletContextClosedException, IOException, RedirectException {
		ClassLoader origClassLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(XMLSpider.class.getClassLoader());
		try {
			PageNode p = ctx.getPageMaker().getPageNode(XMLSpider.pluginName, ctx);
			HTMLNode pageNode = p.outer;
			HTMLNode contentNode = p.content;
	
			WebPage page = new MainPage(spider);
	
			page.processPostRequest(request, contentNode);
			page.writeContent(request, contentNode);
	
			writeHTMLReply(ctx, 200, "OK", null, pageNode.generate());
		} finally {
			Thread.currentThread().setContextClassLoader(origClassLoader);
		}
	}
}
