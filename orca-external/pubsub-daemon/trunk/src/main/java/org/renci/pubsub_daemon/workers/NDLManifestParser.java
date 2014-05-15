package org.renci.pubsub_daemon.workers;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.renci.pubsub_daemon.Globals;

import orca.ndl.INdlManifestModelListener;
import orca.ndl.INdlRequestModelListener;
import orca.ndl.NdlCommons;
import orca.ndl.NdlException;
import orca.ndl.NdlManifestParser;
import orca.ndl.NdlRequestParser;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Resource;

/**
 * Parse NDL manifest for things relevant to blowhole needs
 * @author ibaldin
 *
 */
public class NDLManifestParser implements INdlManifestModelListener, INdlRequestModelListener {

    private static final String UNKNOWN_SLICE = "urn:publicid:IDN+exogeni.net+slice+unknown";
    private static final String UNKNOWN_USER = "urn:publicid:IDN+exogeni.net+user+unknown";
    private static final String UNKNOWN = "unknown";
    
    private static Pattern pattern = Pattern.compile("\\[(.+),(.+)\\]");
	
	// some slice-level details
	protected String sliceUrn = null;
	protected String sliceUuid = null;
	protected String sliceState = null;
	protected Date creationTime = null;
	protected Date expirationTime = null;
	protected String creatorUrn = null;
	
	private boolean requestPhase = true;
	
	private String ndlMan = null;
	
	private NDLManifestParser() {
		
	}
	
	public NDLManifestParser(String sliceUrn, String ndlMan) {
		this.sliceUrn = sliceUrn;
		this.ndlMan = ndlMan;
	}

	/**
	 * Parse both request and manifest
	 * @param ndlMan
	 * @throws Exception
	 */
	public void parseAll() throws Exception {
		
		// first parse it as a request
		// Warning: exceptions may be thrown here, so ignore for now
		try {			
			NdlRequestParser nrp = new NdlRequestParser(ndlMan, this);
			// something wrong with request model that is part of manifest
			// some interfaces belong only to nodes, and no connections
			// for now do less strict checking so we can get IP info
			// 07/2012/ib
			nrp.doLessStrictChecking();
			nrp.processRequest();
			
			nrp.freeModel();
		} catch (NdlException e) {
			Globals.error("Exception encountered while parsing request part of manifest: " + e);
			// FIXME:
		}
		
		parseNoRequest();
	}
	
	/**
	 * Parse only the manifest portion (no request)
	 * @param ndlMan
	 * @throws Exception
	 */
	public void parseNoRequest() throws Exception {
		// second parse it as manifest
		try {
			requestPhase = false;
			NdlManifestParser nmp = new NdlManifestParser(ndlMan, this);

			// this will call the callbacks
			nmp.processManifest();
		    
			nmp.freeModel();
			
		} catch (NdlException e) {
			Globals.error("Exception encountered while parsing manifest: " + e);
			// FIXME:
			return;
		} 
	}
	
//	protected String sliceUrn = null;
//	protected String sliceUuid = null;
//	protected String sliceState = null;
//	protected Date creationTime = null;
//	protected Date expirationTime = null;
//	protected String creatorUrn = null;
	public String getSliceUrn() {
		return (sliceUrn != null ? sliceUrn : UNKNOWN_SLICE);
	}
	
	public String getSliceUuid() {
		return (sliceUuid != null ? sliceUuid : UNKNOWN);
	}
	
	public String getSliceState() {
		return (sliceState != null ? sliceState: UNKNOWN);
	}
	
	public Date getCreationTime() {
		return creationTime;
	}
	
	public Date getExpirationTime() {
		return expirationTime;
	}
	
	public String getCreatorUrn() {
		return (creatorUrn != null ? creatorUrn : UNKNOWN_USER);
	}
	/**
	 * NDL-OWL Parsing
	 */
	
	public void ndlInterface(Resource l, OntModel om, Resource conn,
			Resource node, String ip, String mask) {
	}

	public void ndlCrossConnect(Resource c, OntModel m, long bw, String label,
			List<Resource> interfaces, Resource parent) {
	}

	public void ndlLinkConnection(Resource c, OntModel m,
			List<Resource> interfaces, Resource parent) {
	}

	public void ndlManifest(Resource i, OntModel m) {
		// find out slice-wide creator
		creatorUrn = NdlCommons.getDNProperty(i);
		if (creatorUrn == null)
			creatorUrn = UNKNOWN_USER;
		else {
			// massage DN because it has more than one urn in it
			Matcher matcher = pattern.matcher(creatorUrn);
			if (matcher.matches() && (matcher.groupCount() == 2)) {
				creatorUrn = matcher.group(1).trim();
			}
		}
		
	}

	public void ndlNetworkConnection(Resource l, OntModel om, long bandwidth,
			long latency, List<Resource> interfaces) {

	}

	public void ndlNode(Resource ce, OntModel om, Resource ceClass,
			List<Resource> interfaces) {

	}

	public void ndlNetworkConnectionPath(Resource c, OntModel m,
			List<List<Resource>> path, List<Resource> roots) {
		
	}
	
	public void ndlParseComplete() {

	}

	//
	// Request listener implementation
	public void ndlBroadcastConnection(Resource bl, OntModel om, 
			long bandwidth, List<Resource> interfaces) {
		
	}

	public void ndlNodeDependencies(Resource ni, OntModel m,
			Set<Resource> dependencies) {
		
	}

	public void ndlReservation(Resource i, OntModel m) {
		// (Reservation)->ndl:hasName,ndl:hasGUID,ndl:hasDN -> (string);
		// (Reservation)->geni:hasSliceGeniState->(geni:SliceGeniState);
		
		String newSliceUrn = NdlCommons.getNameProperty(i);
		if (newSliceUrn != null)
			sliceUrn = newSliceUrn;
		
		sliceUuid = NdlCommons.getGuidProperty(i);
		
		sliceState = NdlCommons.getGeniSliceStateName(i);
	}

	public void ndlReservationEnd(Literal e, OntModel m, Date end) {
		expirationTime = end;
		
	}

	public void ndlReservationResources(List<Resource> r, OntModel m) {
		
	}

	public void ndlReservationStart(Literal s, OntModel m, Date start) {
		creationTime = start;
		
	}

	public void ndlReservationTermDuration(Resource d, OntModel m, int years,
			int months, int days, int hours, int minutes, int seconds) {
		if (creationTime == null)
			return;
		Calendar cal = Calendar.getInstance();
		cal.setTime(creationTime);
		cal.add(Calendar.YEAR, years);
		cal.add(Calendar.MONTH, months);
		cal.add(Calendar.DAY_OF_YEAR, days);
		cal.add(Calendar.HOUR, hours);
		cal.add(Calendar.MINUTE, minutes);
		cal.add(Calendar.SECOND, seconds);
		expirationTime = cal.getTime();
	}

	public void ndlSlice(Resource sl, OntModel m) {
		
	}
}
