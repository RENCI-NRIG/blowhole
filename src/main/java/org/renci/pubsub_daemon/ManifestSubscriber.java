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
import java.util.concurrent.Semaphore;

import javax.xml.xpath.XPath;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.jivesoftware.smackx.pubsub.Subscription;


/**
 *
 * @author ibaldin
 */
public class ManifestSubscriber {
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

	static XPath xp;

	private Properties prefProperties = null;
	private SliceListEventListener sliceListener = null;
	private Set<SubscriptionPair> subscriptions = null;
	private Semaphore sem = new Semaphore(1);
	
	static class SubscriptionPair {
		public Subscription sub;
		public String node;
		SubscriptionPair(String n, Subscription s) {
			sub = s;
			node = n;
		}
	}
	
	ManifestSubscriber() {
		processPreferences();

		// create logger
		PropertyConfigurator.configure(prefProperties);
		Logger logger = Logger.getLogger(this.getClass());
		
		Globals.getInstance().setLogger(logger);
		
		try {
			sem.acquire();
		} catch (InterruptedException e) {
			
		}
		addShutDownHandler();
		
		String converters = prefProperties.getProperty(PUBSUB_CONVERTER_LIST);
		if (converters == null) {
			logger.error("You must specify " + PUBSUB_CONVERTER_LIST + " - a comma-separated list of NDL converter URLs");
			System.exit(1);
		}
		
		Globals.getInstance().setConverters(converters);
		
		Globals.info("Creating XMPP connection for new account creation");
		XMPPPubSub xmppAcctCreation = prepareXMPPForAcctCreation();
		if (xmppAcctCreation == null) {
			logger.error("Unable to create XMPP object for creating new accounts");
			System.exit(1);
		}

		xmppAcctCreation.createAccountAndDisconnect();

		XMPPPubSub xmpp = prepareXMPP();

		if (xmpp == null) {
			logger.error("Unable to create xmpp connection, exiting");
			System.exit(1);
		}
		
		Globals.getInstance().setXMPP(xmpp);
		
		// get the list of nodes that list manifests
		List<String> smNodes = getSMNodeList();
		
		subscriptions = new HashSet<SubscriptionPair>();
		// and subscribe to them
		sliceListener = new SliceListEventListener();
		for (String smListNode: smNodes) {
			logger.info("Subscribing to " + smListNode);
			SubscriptionPair sp = new SubscriptionPair(smListNode, xmpp.subscribeToNode(smListNode, sliceListener));
			subscriptions.add(sp);
		}		
		sem.release();
	}
	
	protected void finalize() {
		
		Globals.info("Shutting down subscriptions");
		for(SubscriptionPair s: subscriptions) {
			Globals.getInstance().getXMPP().unsubscribeFromNode(s.node, s.sub);
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
			Globals.error("Missing GMOC.pubsub properties (server:port, login and password must be specified)");
			return null;
		}

		int port = Integer.parseInt(xmppServerPort.split(":")[1]);

		String xmppUseCertificate = prefProperties.getProperty(PUBSUB_USECERTIFICATE_PROP);
		if((xmppUseCertificate == null) || (xmppUseCertificate.equalsIgnoreCase("false"))) {
			if (xmppPassword == null) {
				Globals.error("Missing property GMOC.pubsub.password");
				return null;
			}
			xps = new XMPPPubSub(xmppServerPort.split(":")[0], port, xmppLogin, xmppPassword, 
					PUBSUB_SUBSCRIBER_RESOURCE, Globals.getInstance().getLogger());
		}
		else if((xmppUseCertificate.equalsIgnoreCase("true"))){

			String kspath = prefProperties.getProperty(PUBSUB_KEYSTOREPATH_PROP);
			String kstype = prefProperties.getProperty(PUBSUB_KEYSTORETYPE_PROP);
			String kspass = prefProperties.getProperty(PUBSUB_KEYSTORE_PASS);
			//String tspath = prefProperties.getProperty(PUBSUB_TRUSTSTOREPATH_PROP);
			//String tspass = prefProperties.getProperty(PUBSUB_KEYSTORE_PASS);

			if((kspath == null) || (kstype == null) || (kspass == null)){
				Globals.error("Missing keystore path, keystore type or password for certificate-based login");
				Globals.error("Specify GMOC.pubsub.keystorepath , GMOC.pubsub.keystoretype and GMOC.pubsub.keystorepass properties in measurement.properties");
				return null;
			}

			xps = new XMPPPubSub(xmppServerPort.split(":")[0], port,
					xmppLogin, xmppPassword, kspath, kstype, kspath, kspass, 
					PUBSUB_SUBSCRIBER_RESOURCE, Globals.getInstance().getLogger());
		}
		else {
			Globals.info("Certificate usage property has to be specified as: GMOC.pubsub.usecertificate=[true|false]");
			return null;
		}
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
			Globals.error("Missing GMOC.pubsub properties (server:port and login must be specified)");
			return null;
		}

		int port = Integer.parseInt(xmppServerPort.split(":")[1]);

		String defaultPassword = "defaultpass";
		xps = new XMPPPubSub(xmppServerPort.split(":")[0], port, xmppLogin, defaultPassword, Globals.getInstance().getLogger());

		return xps;
	}

	// list nodes of interest 
	private List<String> getSMNodeList() {
		List<String> ret = new ArrayList<String>();
		
		String smPropVal = prefProperties.getProperty(PUBSUB_SMS_PROP);
		
		if (smPropVal == null) {
			// include everything
			Globals.warn(PUBSUB_SMS_PROP + " not specified, will subscribe to ALL SMs found on this server.");
			smPropVal = "";
		} else if (smPropVal.length() == 0)
			Globals.warn(PUBSUB_SMS_PROP + " is empty, will subscribe to ALL SMs found on this server.");
		
		Set<String> smsOfInterest = new HashSet<String>();

		for(String s: smPropVal.split(",")) {
			smsOfInterest.add(s.trim());
		}
		
		List<String> pubNodes = Globals.getInstance().getXMPP().listAllNodes();
		
		for (String n: pubNodes) {
			//logger.info("Found node " + n);
			for (String sm: smsOfInterest) {
				if (n.matches(ORCA_SM_PREFIX + sm + ORCA_SM_SLICE_LIST_SUFFIX))
					ret.add(n);
			}
		}
		
		return ret;
	}
	
	private void addShutDownHandler() {
		Runtime.getRuntime().addShutdownHook(new Thread (){
			@Override
			public void run() {
				synchronized(Globals.getInstance()) {
					try {
						sem.acquire();
					} catch (InterruptedException e) {
						
					}
					Globals.info("Shutting down subscriptions");
					Globals.getInstance().setShuttingDown();
					for(SubscriptionPair s: subscriptions) {
						if ((s.node != null) && (s.sub != null)) {
							Globals.info("  " + s.node);
							Globals.getInstance().getXMPP().unsubscribeFromNode(s.node, s.sub);
						}
					}
					sliceListener.finalize();
					Globals.info("Exiting");
				}
			}
		});
	}
	
	public static void main(String[] args) {

		ManifestSubscriber ss = new ManifestSubscriber();
		
		synchronized(ss) {
			try {
				ss.wait();
			} catch(InterruptedException ie) {
				Globals.info("Exiting");
			}
		}
	}
}
