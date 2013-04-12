package org.renci.pubsub_daemon;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimerTask;

import org.renci.pubsub_daemon.ManifestSubscriber.SubscriptionPair;

/**
 * Periodic thread that 
 * @author ibaldin
 *
 */
public class ResubscribeThread extends TimerTask {
	private SliceListEventListener sll;
	private ManifestEventListener ml;
	private Set<String> remainingSliceLists = new HashSet<String>();
	private Set<String> remainingManifests = new HashSet<String>();
	
	public ResubscribeThread(SliceListEventListener _sll, ManifestEventListener _ml) {
		sll = _sll;
		ml = _ml;
	}
	
	@Override
	public void run() {
		// go through the sets and try to resubscribe
		
		// for now only slice lists
		List<String> success = new ArrayList<String>();
		Globals.info("Resubscribing to slice lists: ");
		for(String smListNode:remainingSliceLists) {
			Globals.info("  " + smListNode);
			SubscriptionPair sp = new SubscriptionPair(smListNode, 
					Globals.getInstance().getXMPP().subscribeToNode(smListNode, sll));
			if (sp.sub != null) {
				Globals.info("SUCCESS!");
				Globals.getInstance().addSubscription(sp);
				success.add(smListNode);
			} else 
				Globals.info("UNABLE, continuing!");
		}
		for (String s: success) {
			remainingSliceLists.remove(s);
		}
	}

	public synchronized void updateSliceList(final List<String> lst) {
		if (lst == null) {
			remainingSliceLists.clear();
			return;
		}
		for(String n: lst) {
			remainingSliceLists.add(n);
		}
	}
	
	public synchronized void updateManifestList(final List<String> lst) {
		if (lst == null) {
			remainingManifests.clear();
			return;
		}
		for (String n: lst) {
			remainingManifests.add(n);
		}
	}
}
