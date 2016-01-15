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
	private Set<String> smsOfInterest = new HashSet<String>();
	private Set<String> remainingSliceLists = new HashSet<String>();
	private Set<String> remainingManifests = new HashSet<String>();
	
	public ResubscribeThread(SliceListEventListener _sll, ManifestEventListener _ml) {
		sll = _sll;
		ml = _ml;
	}
	
	@Override
	public void run() {
		// go through the sets and try to resubscribe
		
		Globals.info("Getting updated list of available SM slice lists");
		List<String> newSmNodes = Globals.getSMNodeList(smsOfInterest);
		remainingSliceLists.addAll(newSmNodes);
		
		// filter out known subscriptions
		Set<SubscriptionPair> subs = Globals.getInstance().getSubscriptions();
		for(SubscriptionPair sub: subs) {
			remainingSliceLists.remove(sub.node);
		}
		
		// try to subscribe
		List<String> success = new ArrayList<String>();
		Globals.info("Trying to (re)subscribe to slice lists: ");
		
		for(String smListNode:remainingSliceLists) {
			Globals.info("  " + smListNode);
			SubscriptionPair sp = new SubscriptionPair(smListNode, 
					Globals.getInstance().getXMPP().subscribeToNode(smListNode, sll));
			if (sp.sub != null) {
				Globals.info("  SUCCESS!");
				Globals.getInstance().addSubscription(sp);
				success.add(smListNode);
			} else 
				Globals.info("  UNABLE, will try again later!");
		}
		remainingSliceLists.removeAll(success);
	}

	public synchronized void updateSliceList(final Set<String> sms, final List<String> missing) {
		Globals.info("Adding SMs of interest: " + sms);
		smsOfInterest.addAll(sms);
		if (missing == null) {
			remainingSliceLists.clear();
			return;
		}
		Globals.info("Adding slice list for later attempts: " + missing);
		remainingSliceLists.addAll(missing);
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
