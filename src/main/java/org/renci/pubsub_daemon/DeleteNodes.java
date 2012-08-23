package org.renci.pubsub_daemon;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Delete all nodes in pubsub space
 * @author ibaldin
 *
 */
public class DeleteNodes extends ManifestSubscriber {

	private DeleteNodes() {
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
		
		xmpp.deleteAllNodes();
	}
	
	public static void main(String[] args) {
		
		Globals.info("Deleting all pubsub nodes");
		new DeleteNodes();
		Globals.info("Done");
		
	}
}
