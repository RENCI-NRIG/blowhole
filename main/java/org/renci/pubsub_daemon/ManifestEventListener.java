package org.renci.pubsub_daemon;

import java.io.StringReader;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;

import org.apache.log4j.Logger;
import org.jivesoftware.smackx.pubsub.Item;
import org.jivesoftware.smackx.pubsub.ItemPublishEvent;
import org.jivesoftware.smackx.pubsub.listener.ItemEventListener;
import org.xml.sax.InputSource;

public class ManifestEventListener implements ItemEventListener<Item> {
	private final Logger logger;
	private final ExecutorService exec;
	private final String converters;
	private final Boolean compression;
	private Pattern sliceNodePat;
	
	ManifestEventListener(String converters, Boolean compression, Logger l) {
		logger = l;
		this.converters = converters;
		this.compression = compression;
		
		// create a threadpool
		exec = Executors.newCachedThreadPool();	
		
		// thread-safe
		sliceNodePat = Pattern.compile("^/orca/sm/.+---.+/(.+)---(.+)/manifest$");
	}
	
	public void handlePublishedItems(ItemPublishEvent<Item> item) {
		// get the manifest, de-base64. unzip, convert it and push it to GMOC
		try {
			List<Item> items = item.getItems();
			Iterator<Item> it = items.iterator();

			while(it.hasNext()) {
				String itemXml = null;
				try {
					itemXml = (String) it.next().toXML();
				}
				catch (Exception e){
					logger.error("Exception for toXML: " + e);
				}

				logger.info("Received publish event on " + item.getNodeId());
				InputSource is = new InputSource(new StringReader(itemXml));
				XPath xp = XPathFactory.newInstance().newXPath();
				String gzippedManifest = xp.evaluate("/item", is);

				// get slice urn/name from item
				// create/compile pattern
				Matcher matcher = sliceNodePat.matcher(item.getNodeId().trim());
				boolean found = matcher.find();
				
				if (!found) {
					logger.error("Unable to determine the pattern of the published slice name " + item.getNodeId());
					return;
				}
				
				String sliceUrn = matcher.group(1);

				// spawn a thread from a pool
				exec.execute(new TranslateManifestThread(gzippedManifest, sliceUrn, converters, compression, logger));
			}
		} catch (Exception e) {
			logger.error("Unable to parse item XML: " + e);
			e.printStackTrace();
		}
	}

}
