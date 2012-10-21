package org.renci.pubsub_daemon;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;

import org.apache.log4j.Logger;

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
	
	/** 
	 * Disallow
	 */
	private Globals() {
		since = new Date();
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
		manifestsSubscribed --;
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
}
