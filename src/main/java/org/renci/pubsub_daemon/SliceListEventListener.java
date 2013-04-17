package org.renci.pubsub_daemon;

import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;

import org.jivesoftware.smackx.pubsub.Item;
import org.jivesoftware.smackx.pubsub.ItemPublishEvent;
import org.jivesoftware.smackx.pubsub.Subscription;
import org.jivesoftware.smackx.pubsub.listener.ItemEventListener;
import org.xml.sax.InputSource;

/**
 * Deal with changes of manifest lists
 * @author ibaldin
 *
 */
public class SliceListEventListener implements ItemEventListener<Item> {
	private final Map<String, Set<String>> sliceLists = new HashMap<String, Set<String>>();
	private final Map<String, Subscription> subscriptions = new HashMap<String, Subscription>();
	
	SliceListEventListener() {
	}
	
	/**
	 * Unsubscribe from all nodes. Save the node names into list if not null
	 * @param save
	 */
	public void unsubscribeAll(List<String> save) {
		synchronized(this) {
			Globals.info("Unsubscribing from all manifests");
			for (String p: subscriptions.keySet()) {
				if (subscriptions.get(p) != null) {
					Globals.info("    " + p);
					Globals.getInstance().getXMPP().unsubscribeFromNode(p, subscriptions.get(p));
					if (save != null)
						save.add(p);
				}
			}
			subscriptions.clear();
			sliceLists.clear();
		}
	}
	
	/**
	 * d-tor
	 */
	protected void finalize() {
		unsubscribeAll(null);
	}
	
	// non-destructive set diff ret = s1 \ s2
	private Set<String> diffSets(Set<String> s1, Set<String> s2) {
		Set<String> ret = new HashSet<String>(s1);

		ret.removeAll(s2);
		
		return ret;
	}
	
	/**
	 * Use node name and entry from slice list to create a slice manifest node name
	 * @param listSlicesNode
	 * @param manifestEntry
	 * @return
	 */
	private String getManifestNodeName(String listSlicesNode, String manifestEntry) {
		// strip off 'sliceList' and add name--guid/manifest
		if ((listSlicesNode == null) || (manifestEntry == null))
			return null;
		
		String ret = listSlicesNode.replaceAll("sliceList", "");
		
		String[] manifestEntryAr = manifestEntry.split("/");
		
		if (manifestEntryAr.length != 5)
			return null;
		
		return ret + manifestEntryAr[0].trim() + "---" + manifestEntryAr[1].trim() + "/manifest";
	}
	
	public void handlePublishedItems(ItemPublishEvent<Item> item) {
		if (Globals.getInstance().isShuttingDown())
			return;
		try {
			List<Item> items = item.getItems();
			Iterator<Item> it = items.iterator();

			while(it.hasNext()) {
				String itemXml = null;
				try{
					itemXml = (String) it.next().toXML();
				}
				catch (Exception e){
					Globals.error("Exception for toXML: " + e);
				}

				Globals.info("Received publish event on " + item.getNodeId());
				InputSource is = new InputSource(new StringReader(itemXml));
				XPath xp = XPathFactory.newInstance().newXPath();
				String listOfManifests = xp.evaluate("/item", is);
				
				if (listOfManifests == null) {
					Globals.warn("Null list of slices received, continuing");
					continue;
				}
				
				String[] manifests = listOfManifests.split("\n");
				Set<String> newManifestSet = new HashSet<String>();
				
				Globals.debug("SM Manifest node list: ");
				for(String m: manifests) {
					// transform entries into node names
					String t = getManifestNodeName(item.getNodeId(), m);
					if (t != null) {
						Globals.debug("\t" + t);
						newManifestSet.add(t);
					}
				}
				Globals.debug("Checking existing manifest subscriptions for " + item.getNodeId());
				if (sliceLists.get(item.getNodeId()) != null) {
					Globals.debug("SM existing subscriptions list: ");
					for (String m: sliceLists.get(item.getNodeId())) {
						Globals.debug("\t" + m);
					}
				} else
					Globals.debug("\tNo active subscriptions");
				
				synchronized(this) {
					if (sliceLists.get(item.getNodeId()) != null) {
						// slices may have arrived or gone away
						// if they arrived, subscribe manifest event listener
						// if they went away, unsubscribe it

						// are there any new or removed elements
						Set<String> newElems = diffSets(newManifestSet, sliceLists.get(item.getNodeId()));
						Set<String> goneElems = diffSets(sliceLists.get(item.getNodeId()), newManifestSet);

						// unsubscribe from old
						for (String s: goneElems) {
							Globals.info("Removing subscription from manifest " + s);
							Globals.getInstance().getXMPP().unsubscribeFromNode(s, subscriptions.get(s));
							subscriptions.remove(s);
							Globals.getInstance().decManifests();
						}
						// subscribe to new
						for (String s: newElems) {
							Globals.info("Adding subscription to manifest: " + s);
							subscriptions.put(s, Globals.getInstance().getXMPP().subscribeToNode(s, Globals.getInstance().getManifestListener()));
							if (subscriptions.get(s) != null)
								Globals.getInstance().incManifests();
							else
								subscriptions.remove(s);
						}
					} else {
						// subscribe to all
						for (String s: newManifestSet) {
							Globals.info("Adding subscription to manifest from clean slate: " + s);
							subscriptions.put(s, Globals.getInstance().getXMPP().subscribeToNode(s, Globals.getInstance().getManifestListener()));
							if (subscriptions.get(s) != null)
								Globals.getInstance().incManifests();
							else
								subscriptions.remove(s);
						}
					}
					// (re)place the set in map
					sliceLists.put(item.getNodeId(), newManifestSet);
				}
			}
		} catch (Exception e) {
			Globals.error("Unable to parse item XML: " + e);
			e.printStackTrace();
		}
	}
}
