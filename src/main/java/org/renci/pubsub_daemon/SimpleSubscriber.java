/*
 * Copyright 2012 RENCI/UNC-Chapel Hill
 * 
 * @author: ibaldin
 */

package org.renci.pubsub_daemon;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.xml.xpath.XPath;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.jivesoftware.smackx.pubsub.Subscription;

import sun.misc.Signal;
import sun.misc.SignalHandler;


/**
 *
 * @author ibaldin
 */
public class SimpleSubscriber {
	private static final String ORCA_SM_SLICE_LIST_SUFFIX = ".+sliceList";
	private static final String ORCA_SM_PREFIX = "/orca/sm/";

	private static final String PUBSUB_SUBSCRIBER_RESOURCE = "GMOC-Subscriber";
	private static final String PREF_FILE = ".xmpp.properties";
	private static final String PUBSUB_PROP_PREFIX = "GMOC.pubsub";
	private static final String PUBSUB_SERVER_PROP = PUBSUB_PROP_PREFIX + ".server";
	private static final String PUBSUB_LOGIN_PROP = PUBSUB_PROP_PREFIX + ".login";
	private static final String PUBSUB_PASSWORD_PROP = PUBSUB_PROP_PREFIX + ".password";
	private static final String PUBSUB_SMS_PROP = PUBSUB_PROP_PREFIX + ".monitored.sm.list";
	private static final String PUBSUB_CONVERTER_LIST = PUBSUB_PROP_PREFIX + ".ndl.converter.list";

	// For certificate based login
	private static final String PUBSUB_USECERTIFICATE_PROP = PUBSUB_PROP_PREFIX + ".usecertificate";
	private static final String PUBSUB_KEYSTOREPATH_PROP = PUBSUB_PROP_PREFIX + ".keystorepath";
	private static final String PUBSUB_KEYSTORETYPE_PROP = PUBSUB_PROP_PREFIX + ".keystoretype";
	private static final String PUBSUB_KEYSTORE_PASS = PUBSUB_PROP_PREFIX + ".keystorepass";
	// Note truststore password would be read from the GMOC.pubsub.password property when using certificates

	static Logger logger;
	static XPath xp;

	private Properties prefProperties = null;
	private final XMPPPubSub xmpp;
	private SliceListEventListener sliceListener = null;
	private Set<SubscriptionPair> subscriptions = null;

	static class SubscriptionPair {
		public Subscription sub;
		public String node;
		SubscriptionPair(String n, Subscription s) {
			sub = s;
			node = n;
		}
	}
	
	SimpleSubscriber() {
		processPreferences();

		// create logger
		PropertyConfigurator.configure(prefProperties);
		logger = Logger.getLogger(this.getClass());
		
		String converters = prefProperties.getProperty(PUBSUB_CONVERTER_LIST);
		if (converters == null) {
			logger.error("You must specify " + PUBSUB_CONVERTER_LIST + " - a comma-separated list of NDL converter URLs");
			System.exit(1);
		}
		
		logger.info("Creating XMPP connection for new account creation");
		XMPPPubSub xmppAcctCreation = prepareXMPPForAcctCreation();
		if (xmppAcctCreation == null) {
			logger.error("Unable to create XMPP object for creating new accounts");
			System.exit(1);
		}

		xmppAcctCreation.createAccountAndDisconnect();

		xmpp = prepareXMPP();

		if (xmpp == null) {
			logger.error("Unable to create xmpp connection, exiting");
			System.exit(1);
		}
		
		// get the list of nodes that list manifests
		List<String> smNodes = getSMNodeList();
		
		subscriptions = new HashSet<SubscriptionPair>();
		// and subscribe to them
		sliceListener = new SliceListEventListener(xmpp, converters, true, logger);
		for (String smListNode: smNodes) {
			logger.info("Subscribing to " + smListNode);
			SubscriptionPair sp = new SubscriptionPair(smListNode, xmpp.subscribeToNode(smListNode, sliceListener));
			subscriptions.add(sp);
		}
	}
	
	protected void finalize() {
		for(SubscriptionPair s: subscriptions) {
			xmpp.unsubscribeFromNode(s.node, s.sub);
		}
		sliceListener.finalize();
	}

	/**
	 * Read and process preferences file
	 */
	private void processPreferences() {
		Properties p = System.getProperties();

		String prefFilePath = "" + p.getProperty("user.home") + p.getProperty("file.separator") + PREF_FILE;
		try {
			File prefs = new File(prefFilePath);
			FileInputStream is = new FileInputStream(prefs);
			BufferedReader bin = new BufferedReader(new InputStreamReader(is, "UTF-8"));

			prefProperties = new Properties();
			prefProperties.load(bin);

		} catch (IOException e) {
			;
		}
	}

	/**
	 * prepare XMPP  object
	 * @param args
	 */
	XMPPPubSub prepareXMPP() {

		String xmppServerPort = prefProperties.getProperty(PUBSUB_SERVER_PROP);
		String xmppLogin = prefProperties.getProperty(PUBSUB_LOGIN_PROP);
		String xmppPassword = prefProperties.getProperty(PUBSUB_PASSWORD_PROP);

		XMPPPubSub xps = null;

		if ((xmppServerPort == null) ||
				(xmppLogin == null)) {
			logger.error("Missing GMOC.pubsub properties (server:port, login and password must be specified)");
			return null;
		}

		int port = Integer.parseInt(xmppServerPort.split(":")[1]);

		String xmppUseCertificate = prefProperties.getProperty(PUBSUB_USECERTIFICATE_PROP);
		if((xmppUseCertificate == null) || (xmppUseCertificate.equalsIgnoreCase("false"))) {
			if (xmppPassword == null) {
				logger.error("Missing property GMOC.pubsub.password");
				return null;
			}
			xps = new XMPPPubSub(xmppServerPort.split(":")[0], port, xmppLogin, xmppPassword, PUBSUB_SUBSCRIBER_RESOURCE, logger);
		}
		else if((xmppUseCertificate.equalsIgnoreCase("true"))){

			String kspath = prefProperties.getProperty(PUBSUB_KEYSTOREPATH_PROP);
			String kstype = prefProperties.getProperty(PUBSUB_KEYSTORETYPE_PROP);
			String kspass = prefProperties.getProperty(PUBSUB_KEYSTORE_PASS);
			//String tspath = prefProperties.getProperty(PUBSUB_TRUSTSTOREPATH_PROP);
			//String tspass = prefProperties.getProperty(PUBSUB_KEYSTORE_PASS);

			if((kspath == null) || (kstype == null) || (kspass == null)){
				logger.error("Missing keystore path, keystore type or password for certificate-based login");
				logger.error("Specify GMOC.pubsub.keystorepath , GMOC.pubsub.keystoretype and GMOC.pubsub.keystorepass properties in measurement.properties");
				return null;
			}

			xps = new XMPPPubSub(xmppServerPort.split(":")[0], port,
					xmppLogin, xmppPassword, kspath, kstype, kspath, kspass, PUBSUB_SUBSCRIBER_RESOURCE, logger);
		}
		else {
			logger.info("Certificate usage property has to be specified as: GMOC.pubsub.usecertificate=[true|false]");
			return null;
		}

		// **TODO** Remove this after testing
		// xps.login();
		// logger.info("Login session done");
		//
		return xps;

	}

	/**
	 *
	 * @param mp
	 * @return
	 */

	private XMPPPubSub prepareXMPPForAcctCreation() {

		String xmppServerPort = prefProperties.getProperty(PUBSUB_SERVER_PROP);
		String xmppLogin = prefProperties.getProperty(PUBSUB_LOGIN_PROP);

		XMPPPubSub xps = null;

		if ((xmppServerPort == null) ||
				(xmppLogin == null)) {
			logger.error("Missing GMOC.pubsub properties (server:port and login must be specified)");
			return null;
		}

		int port = Integer.parseInt(xmppServerPort.split(":")[1]);

		String defaultPassword = "defaultpass";
		xps = new XMPPPubSub(xmppServerPort.split(":")[0], port, xmppLogin, defaultPassword, logger);

		return xps;
	}

	// list nodes of interest 
	private List<String> getSMNodeList() {
		List<String> ret = new ArrayList<String>();
		
		String smPropVal = prefProperties.getProperty(PUBSUB_SMS_PROP);
		
		Set<String> smsOfInterest = new HashSet<String>();
		if (smPropVal != null) {
			for(String s: smPropVal.split(",")) {
				smsOfInterest.add(s.trim());
			}
		}
		
		List<String> pubNodes = xmpp.listAllNodes();
		
		for (String n: pubNodes) {
			//logger.info("Found node " + n);
			for (String sm: smsOfInterest) {
				if (n.matches(ORCA_SM_PREFIX + sm + ORCA_SM_SLICE_LIST_SUFFIX))
					ret.add(n);
			}
		}
		
		return ret;
	}
	
	@SuppressWarnings("restriction")
	private static class SubSignalHandler implements SignalHandler {
		private SignalHandler oldHandler;
		private SimpleSubscriber ss;
		
	    // Static method to install the signal handler
	    public static SubSignalHandler install(String signalName, SimpleSubscriber ss) {
	        Signal diagSignal = new Signal(signalName);
	        SubSignalHandler diagHandler = new SubSignalHandler();
	        diagHandler.oldHandler = Signal.handle(diagSignal, diagHandler);
	        diagHandler.ss = ss;
	        return diagHandler;
	    }
		
		public void handle(Signal sig) {
			// call finalize explicitly
			
			this.ss.finalize();
			
			// unblock main thread
			this.ss.notifyAll();
			
	         // Chain back to previous handler, if one exists
            if (oldHandler != SIG_DFL && oldHandler != SIG_IGN ) {
                oldHandler.handle(sig);
            }
            System.exit(0);
		}
	}
	
	public static void main(String[] args) {

		SimpleSubscriber ss = new SimpleSubscriber();

		//SubSignalHandler.install("TERM", ss);
		
		synchronized(ss) {
			try {
				ss.wait();
			} catch(InterruptedException ie) {
				logger.info("Exiting");
			}
		}
	}
}
