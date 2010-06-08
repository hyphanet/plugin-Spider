/**
 * Web reuqest handlers
 * 
 * @author j16sdiz (1024D/75494252)
 */
package plugins.Spider.web;

import plugins.Spider.Spider;
import freenet.client.HighLevelSimpleClient;
import freenet.clients.http.PageMaker;
import freenet.clients.http.ToadletContainer;
import freenet.node.NodeClientCore;

public class WebInterface {
	private final Spider spider;
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
	public WebInterface(Spider spider, HighLevelSimpleClient client, ToadletContainer container, NodeClientCore core) {
		this.spider = spider;

		pageMaker = spider.getPageMaker();
		this.toadletContainer = container;
		this.client = client;
		this.core = core;
	}
	
	public void load() {
		pageMaker.addNavigationCategory("/spider/", "Spider", "Spider", spider);
		
		toadletContainer.register(mainToadlet = new MainPageToadlet(client, spider, core), "Spider", "/spider/", true, "Spider", "Spider", true, null);
		toadletContainer.register(configToadlet = new ConfigPageToadlet(client, spider, core), "Spider", "/spider/config", true, "Configure Spider", "Configure Spider", true, null);
	}

	
	public void unload() {
		toadletContainer.unregister(configToadlet);
		toadletContainer.unregister(mainToadlet);
		pageMaker.removeNavigationCategory("Spider");
	}
}