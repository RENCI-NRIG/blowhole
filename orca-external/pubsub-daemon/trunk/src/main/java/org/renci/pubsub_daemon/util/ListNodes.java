package org.renci.pubsub_daemon.util;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.renci.pubsub_daemon.Globals;
import org.renci.pubsub_daemon.ManifestSubscriber;
import org.renci.pubsub_daemon.XMPPPubSub;

/**
 * Delete all nodes in pubsub space
 * @author ibaldin
 *
 */
public class ListNodes extends ManifestSubscriber {

	private ListNodes() {
		super(new Object());
		
		processPreferences();
		
		// create logger
		PropertyConfigurator.configure(prefProperties);
		Logger logger = Logger.getLogger(this.getClass());
		
		Globals.getInstance().setLogger(logger);
		
		XMPPPubSub xmppAcctCreation = prepareXMPPForAcctCreation();
		if (xmppAcctCreation == null) {
			System.err.println("Unable to create XMPP object for creating new accounts");
			System.exit(1);
		}

		xmppAcctCreation.createAccountAndDisconnect();

		XMPPPubSub xmpp = prepareXMPP();

		if (xmpp == null) {
			System.err.println("Unable to create xmpp connection, exiting");
			System.exit(1);
		}
		
		System.out.println("Available nodes:\n\n");
		int count = 0;
		for (String n: xmpp.listAllNodes()) {
			System.out.println(n);
			count++;
		}
		System.out.println("Total: " + count);
	}
	
	public static void main(String[] args) {
		
		new ListNodes();
	}
}
