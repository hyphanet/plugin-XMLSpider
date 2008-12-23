package plugins.XMLSpider.web;

import freenet.support.HTMLNode;
import freenet.support.api.HTTPRequest;

/**
 * Interface for all web pages
 * 
 * @author j16sdiz (1024D/75494252)
 */
interface WebPage {

	public abstract void processPostRequest(HTTPRequest request, HTMLNode contentNode);

	public abstract void writeContent(HTTPRequest request, HTMLNode contentNode);

}