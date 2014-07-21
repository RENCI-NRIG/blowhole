package org.renci.pubsub_daemon;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import orca.ndl_conversion.IMultiFormatConverter;
import orca.ndl_conversion.UniversalNDLConverter;

import org.apache.log4j.Logger;
import org.renci.pubsub_daemon.ManifestSubscriber.SubscriptionPair;
import org.renci.pubsub_daemon.workers.AbstractWorker;

/**
 * global settings singleton
 * @author ibaldin
 *
 */
public class Globals {
	private static final Globals instance = new Globals();
	
	private String converters = null;
	private Boolean compression = true;
	private Boolean debugOn = false;
	private XMPPPubSub xmpp = null;
	private Logger logger = null;
	private Boolean shuttingDown = false;
	private Date since = null;
	private long manifestsSubscribed = 0;
	private long eventsServed = 0;
	private IMultiFormatConverter internalConverter = null;
	
	private SliceListEventListener sll = new SliceListEventListener();
	private ManifestEventListener ml = new ManifestEventListener();
	
	private Set<SubscriptionPair> sliceListSubscriptions = new HashSet<SubscriptionPair>();
	
	private List<AbstractWorker> workers = new LinkedList<AbstractWorker>();
	
	private Properties configProperties = null;
	
	/** 
	 * Disallow
	 */
	private Globals() {
		since = new Date();
	}
	
	public synchronized void addSubscription(SubscriptionPair p) {
		if (p != null)
			sliceListSubscriptions.add(p);
	}
	
	/**
	 * Get a copy of subscriptions
	 * @return
	 */
	public synchronized Set<SubscriptionPair> getSubscriptions() {
		return Collections.unmodifiableSet(sliceListSubscriptions);
	}
	
	public synchronized void clearSubscriptions() {
		sliceListSubscriptions.clear();
	}
	
	public static Globals getInstance() {
		return instance;
	}
	
	public void setConverters(String c) {
		converters = c;
	}
	
	public String getConverters() {
		return converters;
	}
	
	public void setCompression(Boolean c) {
		compression = c;
	}
	
	public Boolean getCompression() {
		return compression;
	}
	
	public void setXMPP(XMPPPubSub x) {
		xmpp = x;
	}
	
	public XMPPPubSub getXMPP() {
		return xmpp;
	}
	
	public void setDebugOn() {
		debugOn = true;
	}
	
	public void setDebugOff() {
		debugOn = false;
	}
	
	public Boolean isDebugOn() {
		return debugOn;
	}
	
	public void setLogger(Logger l) {
		logger = l;
	}
	
	public Logger getLogger() {
		return logger;
	}
	
	public void setShuttingDown() {
		shuttingDown = true;
	}
	
	public Boolean isShuttingDown() {
		return shuttingDown;
	}
	
	public String toString() {
		return "Up since " + since + " subscribed to " + manifestsSubscribed + ", served " + eventsServed + " manifest events";
	}
	
	synchronized void incManifests() {
		manifestsSubscribed ++;
	}
	
	synchronized void decManifests() {
		if (manifestsSubscribed > 0) manifestsSubscribed --;
	}
	
	synchronized void incEvents() {
		eventsServed ++;
	}
	

	
	/*
	 * Shortcuts
	 */
	public static void info(Object s) {
		if (getInstance().getLogger() != null)
			getInstance().getLogger().info(s);
		else
			System.out.println(s);
	}
	public static void warn(Object s) {
		if (getInstance().getLogger() != null)
			getInstance().getLogger().warn(s);
		else
			System.err.println(s);
	}
	public static void debug(Object s) {
		if (getInstance().getLogger() != null)
			getInstance().getLogger().debug(s);
		else
			System.out.println(s);
	}
	public static void error(Object s) {
		if (getInstance().getLogger() != null)
			getInstance().getLogger().error(s);
		else
			System.err.println(s);
	}
	
	public SliceListEventListener getSliceListener() {
		return sll;
	}
	
	public ManifestEventListener getManifestListener() {
		return ml;
	}
	
	public void addWorker(AbstractWorker w) {
		if (w != null)
			workers.add(w);
	}
	
	/**
	 * Parse a comma-separated string, instantiate workers and add them to
	 * internal list
	 * @param workerNames
	 */
	public void setWorkers(String workerNames) {
		if (workerNames == null)
			return;
		
		String[] workerNamesAr = workerNames.split(",");
		
		for(String wn: workerNamesAr) {
			try {
				Class<?> wcl = Class.forName(wn.trim());
				AbstractWorker iw = (AbstractWorker)wcl.newInstance();
				addWorker(iw);
			} catch (ClassNotFoundException cnfe) {
				throw new RuntimeException("Unable to find class " + wn);
			} catch (Exception e) {
				throw new RuntimeException("Unable to instantiate class " + wn + ": " + e);
			}
		}
	}
	
	/**
	 * Returns a copy of a list of registered workers
	 * @return
	 */
	public List<AbstractWorker> getWorkers() {
		return new LinkedList<AbstractWorker>(workers);
	}

	public static void writeToFile(String man, String name) {
		Globals.info("Writing manifest to file " + name);

		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(name));
			bw.write(man);
			bw.close();
		} catch (IOException e) {
			Globals.error("Unable to write manifest to file " + name);
		}
	}
	
	
	public static void writeToFile(String man, File f) {

		try {
			String fName = f.getCanonicalPath();
			Globals.info("Writing manifest to file " + fName);
			BufferedWriter bw = new BufferedWriter(new FileWriter(f));
			bw.write(man);
			bw.close();
		} catch (IOException e) {
			Globals.error("Unable to write manifest to file");
		}
	}
	

	public static String executeCommand(List<String> cmd, Properties env) {
		SystemExecutor se = new SystemExecutor();

		String response = null;
		try {
			response = se.execute(cmd, env, null, (Reader)null);
		} catch (RuntimeException re) {
			Globals.error("Unable to execute command " + cmd + ": " + re);
		}
		return response;
	}
	
	public void setConfigProperties(Properties p) {
		configProperties = p;
	}
	
	/**
	 * Return a configuration property or null
	 * @param name
	 * @return
	 */
	public String getConfigProperty(String name) {
		if (configProperties != null)
			return configProperties.getProperty(name);
		return null;
	}
	
    public static String readFileToString(String path) {
        byte[] buffer = new byte[(int) new File(path).length()];
        BufferedInputStream f = null;
        try {
                f = new BufferedInputStream(new FileInputStream(path));
                f.read(buffer);
        } catch (FileNotFoundException e) {
                System.err.print("Unable to find file");
        } catch (IOException ie) {
                System.err.print("Unable to read file");
        } finally {
                if (f != null) try { f.close(); } catch (IOException ignored) { }
        }
        return new String(buffer);
    }	
    
    /**
     * Not doing it by default because the logger should be present first
     */
    public void createInternalConverter() {
    	if (logger == null)
    		logger = Logger.getLogger(this.getClass());
    	internalConverter = new UniversalNDLConverter(logger);
    }
    
    public IMultiFormatConverter getInternalConverter() {
    	return internalConverter;
    }
}
