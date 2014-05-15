package org.renci.pubsub_daemon;

import java.beans.PropertyVetoException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.renci.pubsub_daemon.ManifestSubscriber.SubscriptionPair;
import org.renci.pubsub_daemon.workers.IWorker;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;

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
	private String publishUrl = null;
	private Date since = null;
	private long manifestsSubscribed = 0;
	private long eventsServed = 0;
	
	private String dbUrl = null;
	private String dbUser = null;
	private String dbPass = null;
	private boolean dbValid = false;
	private ComboPooledDataSource cpds = null;
	
	private SliceListEventListener sll = new SliceListEventListener();
	private ManifestEventListener ml = new ManifestEventListener();
	
	private Set<SubscriptionPair> sliceListSubscriptions = new HashSet<SubscriptionPair>();
	
	private List<IWorker> workers = new LinkedList<IWorker>();
	
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
	
	public void setPublishUrl(String s) {
		publishUrl = s;
	}
	
	public String getPublishUrl() {
		return publishUrl;
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
	
	public void setDbParams(String url, String user, String pass) {
		dbUrl = url;
		dbUser = user;
		dbPass = pass;
		
		// if all non-null, create a pooled connection source to the db
		if ((url == null) || (user == null) || (pass == null)) {
			dbValid = false;
			if (cpds != null) {
				try {
					DataSources.destroy(cpds);
				} catch (SQLException se) {
					error("SQL Error (non fatal): " + se);
				}
				cpds = null;
			}
			info("Insufficient database parameters, not creating a connection");
			return;
		}
		
		cpds = new ComboPooledDataSource();
		try {
			cpds.setDriverClass("com.mysql.jdbc.Driver");
		} catch (PropertyVetoException e) {
			error("Unable to create JDBC MySQL connection (non-fatal): " + e);
			try {
				DataSources.destroy(cpds);
			} catch (SQLException e1) {
			}
			cpds = null;
			dbValid = false;
			return;
		}
		cpds.setJdbcUrl(url);
		cpds.setUser(user);
		cpds.setPassword(pass);
		
		dbValid = true;
	}
	
	// get a new db connection from pool
	public Connection getDbConnection() throws SQLException {
		if (dbValid)
			return cpds.getConnection();
		throw new SQLException("Invalid database parameters");
	}
	
	public boolean isDbValid() {
		return dbValid;
	}
	
	public String getDbUrl() {
		return dbUrl;
	}
	
	public String getDbUser() {
		return dbUser;
	}
	
	public String getDbPass() {
		return dbPass;
	}
	
	/*
	 * Shortcuts
	 */
	public static void info(Object s) {
		if (getInstance().getLogger() != null)
			getInstance().getLogger().info(s);
	}
	public static void warn(Object s) {
		if (getInstance().getLogger() != null)
			getInstance().getLogger().warn(s);
	}
	public static void debug(Object s) {
		if (getInstance().getLogger() != null)
			getInstance().getLogger().debug(s);
	}
	public static void error(Object s) {
		if (getInstance().getLogger() != null)
			getInstance().getLogger().error(s);
	}
	
	public SliceListEventListener getSliceListener() {
		return sll;
	}
	
	public ManifestEventListener getManifestListener() {
		return ml;
	}
	
	public void addWorker(IWorker w) {
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
				IWorker iw = (IWorker)wcl.newInstance();
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
	public List<IWorker> getWorkers() {
		return new LinkedList(workers);
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
}
