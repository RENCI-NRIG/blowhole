package org.renci.pubsub_daemon;

import org.apache.log4j.Logger;

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
	
	/** 
	 * Disallow
	 */
	private Globals() {
		
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