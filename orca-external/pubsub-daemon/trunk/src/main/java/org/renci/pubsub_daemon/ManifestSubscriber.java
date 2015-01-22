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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;

import javax.xml.xpath.XPath;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.jivesoftware.smack.SmackConfiguration;
import org.jivesoftware.smackx.pubsub.Subscription;
import org.renci.pubsub_daemon.workers.AbstractWorker;
import org.renci.xmpp_pubsub.IPubSubReconnectCallback;
import org.renci.xmpp_pubsub.XMPPPubSub;


/**
 *
 * @author ibaldin
 */
public class ManifestSubscriber implements IPubSubReconnectCallback {
	public static final String buildVersion = "Blowhole " + ManifestSubscriber.class.getPackage().getImplementationVersion();
	private static final String ORCA_SM_SLICE_LIST_SUFFIX = ".+sliceList";
	private static final String ORCA_SM_PREFIX = "/orca/sm/";

	private static final String PUBSUB_SUBSCRIBER_RESOURCE = "GMOC-Subscriber";
	public static final String PREF_FILE = ".xmpp.properties";
	public static final String GLOBAL_PREF_FILE = "/etc/blowhole/xmpp.properties";
	public static final String PUBSUB_PROP_PREFIX = "pubsub";
	private static final String PUBSUB_SERVER_PROP = PUBSUB_PROP_PREFIX + ".server";
	private static final String PUBSUB_LOGIN_PROP = PUBSUB_PROP_PREFIX + ".login";
	private static final String PUBSUB_PASSWORD_PROP = PUBSUB_PROP_PREFIX + ".password";
	private static final String PUBSUB_SMS_PROP = PUBSUB_PROP_PREFIX + ".monitored.sm.list";
	public static final String PUBSUB_CONVERTER_LIST = PUBSUB_PROP_PREFIX + ".ndl.converter.list";

	private static final String DEBUG_PROPERTY = "debug";

	private static final String WORKER_LIST = "worker.list";
	
	// For certificate based login
	private static final String PUBSUB_USECERTIFICATE_PROP = PUBSUB_PROP_PREFIX + ".usecertificate";
	private static final String PUBSUB_KEYSTOREPATH_PROP = PUBSUB_PROP_PREFIX + ".keystorepath";
	private static final String PUBSUB_KEYSTORETYPE_PROP = PUBSUB_PROP_PREFIX + ".keystoretype";
	private static final String PUBSUB_KEYSTORE_PASS = PUBSUB_PROP_PREFIX + ".keystorepass";
	// Note truststore password would be read from the GMOC.pubsub.password property when using certificates

	static XPath xp;

	protected Properties prefProperties = null;
	private Semaphore sem = new Semaphore(1);
	private Timer tmr = null;
	private ResubscribeThread rst;
	
	static class SubscriptionPair {
		public Subscription sub;
		public String node;
		SubscriptionPair(String n, Subscription s) {
			sub = s;
			node = n;
		}
	}
	
	public ManifestSubscriber(Object o) {
		// do nothing (for inheritance)
	}
	
	private ManifestSubscriber() {
		processPreferences();

		SmackConfiguration.setLocalSocks5ProxyEnabled(false);
		
		// create logger
		PropertyConfigurator.configure(prefProperties);
		Logger logger = Logger.getLogger(this.getClass());
		
		Globals.getInstance().setLogger(logger);
		Globals.getInstance().setConfigProperties(prefProperties);
		
		// process workers
		String workerNames = prefProperties.getProperty(WORKER_LIST);
		if (workerNames == null) {
			logger.error("You must specify " + WORKER_LIST + " - a comma-separated list of classes implementing worker interfaces");
			System.exit(1);
		}
		
		Globals.getInstance().setWorkers(workerNames);
		
		// process debug setting
		String debugSet = prefProperties.getProperty(DEBUG_PROPERTY);
		if ("yes".equalsIgnoreCase(debugSet) || "true".equalsIgnoreCase(debugSet)) {
			Globals.getInstance().setDebugOn();
		}
		
		try {
			sem.acquire();
		} catch (InterruptedException e) {
			
		}

		Globals.info(buildVersion + " is starting");
		
		String converters = prefProperties.getProperty(PUBSUB_CONVERTER_LIST);
		if ((converters != null) && (converters.length() > 0)) { 
			Globals.getInstance().setConverters(converters);
		} else { 
			// 	create internal NDL converter
			Globals.getInstance().createInternalConverter();
		}
		
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
		
		List<String> missingNodes = new ArrayList<String>();
		Globals.info("Subscribing to known SM manifest lists");
		for (String smListNode: smNodes) {
			logger.info("  " + smListNode);
			SubscriptionPair sp = new SubscriptionPair(smListNode, xmpp.subscribeToNode(smListNode, Globals.getInstance().getSliceListener())); 
			if (sp.sub != null) {
				Globals.info("SUCCESS!");
				Globals.getInstance().addSubscription(sp);
			} else {
				Globals.info("UNABLE, will try again later");
				missingNodes.add(smListNode);
			}
		}		
		rst = new ResubscribeThread(Globals.getInstance().getSliceListener(), Globals.getInstance().getManifestListener());
		
		rst.updateSliceList(missingNodes);
		addShutDownHandler();
		sem.release();
		
		// run startup functions
		for(AbstractWorker w: Globals.getInstance().getWorkers()) {
			Globals.info("Running startup function for " + w.getName());
			w.runAtStartup();
		}
		
		// start a periodic reporting thread
		tmr = new Timer("PubSub Background", true);
		tmr.schedule(new TimerTask() {
			public void run() {
				Globals.info(Globals.getInstance());
			}
		}, 5000, 5000);

		// start resubscribe thread
		tmr.schedule(rst, 5000, 30000);
	}
	
	protected void finalize() {
		
		Globals.info("Shutting down slice list subscriptions");
		unsubscribeAll(null);
		Globals.getInstance().getSliceListener().finalize();
	}

	private Properties loadProperties(String fileName) throws IOException {
		File prefs = new File(fileName);
		FileInputStream is = new FileInputStream(prefs);
		BufferedReader bin = new BufferedReader(new InputStreamReader(is, "UTF-8"));

		Properties p = new Properties();
		p.load(bin);
		bin.close();
		
		return p;
	}
	
	/**
	 * Read and process preferences file
	 */
	protected void processPreferences() {
		Properties p = System.getProperties();

		// properties can be under /etc/blowhole/xmpp.properties or under $HOME/.xmpp.properties
		// in that order of preference
		String prefFilePath = GLOBAL_PREF_FILE;
		
		try {
			prefProperties = loadProperties(prefFilePath);
			return;
		} catch (IOException ioe) {
			System.err.println("Unable to load global config file " + prefFilePath + ", trying local file");
		}
		
		prefFilePath = "" + p.getProperty("user.home") + p.getProperty("file.separator") + PREF_FILE;
		try {
			prefProperties = loadProperties(prefFilePath);
		} catch (IOException e) {
			System.err.println("Unable to load local config file " + prefFilePath + ", exiting.");
			InputStream is = Class.class.getResourceAsStream("/org/renci/pubsub_daemon/xmpp.sample.properties");
			if (is != null) {
				try {
					String s = new java.util.Scanner(is).useDelimiter("\\A").next();
					System.err.println("Create $HOME/.xmpp.properties file as follows: \n\n" + s);
				} catch (java.util.NoSuchElementException ee) {
					;
				}
			} else {
				System.err.println("Unable to load sample properties");
			}
			System.exit(1);
		}
	}

	/**
	 * prepare XMPP  object
	 * @param args
	 */
	protected XMPPPubSub prepareXMPP() {

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
					PUBSUB_SUBSCRIBER_RESOURCE, Globals.getInstance().getLogger(), this);
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

			File ksp = new File(kspath);
			if (!ksp.exists()) {
				Globals.error("JKS file " + kspath + " cannot be found.");
				return null;
			}
			
			xps = new XMPPPubSub(xmppServerPort.split(":")[0], port,
					xmppLogin, xmppPassword, kspath, kstype, kspath, kspass, 
					PUBSUB_SUBSCRIBER_RESOURCE, Globals.getInstance().getLogger(), this);
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

	protected XMPPPubSub prepareXMPPForAcctCreation() {

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
		// callback not needed
		xps = new XMPPPubSub(xmppServerPort.split(":")[0], port, xmppLogin, defaultPassword, Globals.getInstance().getLogger(), null);

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
	
	private void unsubscribeAll(List<String> save) {
		Globals.info("Unsubscribing from all slice lists");
		for(SubscriptionPair s: Globals.getInstance().getSubscriptions()) {
			if ((s.node != null) && (s.sub != null)) {
				Globals.info("  " + s.node);
				Globals.getInstance().getXMPP().unsubscribeFromNode(s.node, s.sub);
				if (save != null)
					save.add(s.node);
			}
		}
		Globals.getInstance().clearSubscriptions();
	}
	
	protected void addShutDownHandler() {
		Runtime.getRuntime().addShutdownHook(new Thread (){
			@Override
			public void run() {
				synchronized(Globals.getInstance()) {
					try {
						sem.acquire();
					} catch (InterruptedException e) {
						
					}
					Globals.info("Shutting down subscriptions");
					if (tmr != null)
						tmr.cancel();
					
					Globals.getInstance().setShuttingDown();
					unsubscribeAll(null);
					Globals.getInstance().getSliceListener().unsubscribeAll(null);
					Globals.info("Exiting");
				}
			}
		});
	}
	

	@Override
	public String name() {
		return "Resubscribing callback";
	}

	@Override
	public void onReconnect() {
		// save subscription nodes
		List<String> listNodes = new ArrayList<String>();
		List<String> manifestNodes = new ArrayList<String>();
		
		// unsubscribe from all
		unsubscribeAll(listNodes);
		Globals.getInstance().getSliceListener().unsubscribeAll(manifestNodes);
		
		// tell resubscription thread
		rst.updateSliceList(listNodes);
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
