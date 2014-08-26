package org.renci.pubsub_daemon.workers;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import orca.ndl.INdlManifestModelListener;
import orca.ndl.INdlRequestModelListener;
import orca.ndl.NdlCommons;
import orca.ndl.NdlException;
import orca.ndl.NdlManifestParser;
import orca.ndl.NdlRequestParser;
import orca.ndl.NdlToRSpecHelper;
import orca.ndl.NdlToRSpecHelper.UrnType;
import orca.ndl_conversion.geni_rspec.manifest3.LinkContents;
import orca.ndl_conversion.geni_rspec.manifest3.NodeContents;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Resource;

/**
 * Parse parts of manifest to support monitoring
 * @author ibaldin
 *
 */
public class GENIWorkerManifestParser implements INdlManifestModelListener, INdlRequestModelListener {

	// indicate whether this interface is from a member of a  node group
	protected Map<Resource, Boolean> groupInterfaceMap;
	// indicate that an interface belongs to a node (main rspec only sees these, 
	// and not any of the intermediate ones)
	protected Set<Resource> nodeInterfaces;
	// save mappings from resources to DOM nodes and links
	protected Map<Resource, NodeContents> resourceToNode;
	protected Map<Resource, LinkContents> resourceToLink;

	// some slice-level details
	protected String sliceUrn = null;
	protected String sliceUuid = null;
	protected String sliceState = null;
	protected Date creationTime = null;
	protected Date expirationTime = null;
	protected String creatorUrn = null;
	protected String controllerUrl = null;
	
	private boolean requestPhase = true;
	
	private Map<String, String> urnToReservation = new HashMap<String, String>();
	
	private GENIWorkerManifestParser() {
		
	}
	
	public GENIWorkerManifestParser(String sliceName) {

		this.groupInterfaceMap = new HashMap<Resource, Boolean>();
		this.nodeInterfaces = new HashSet<Resource>();
		this.resourceToLink = new HashMap<Resource, LinkContents>();
		this.resourceToNode = new HashMap<Resource, NodeContents>();

		this.sliceUrn = sliceName;
	}

	public void parse(String ndlMan) throws Exception {
		
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
			throw new Exception(e.getMessage());
		}
		
		// second parse it as manifest
		try {
			requestPhase = false;
			NdlManifestParser nmp = new NdlManifestParser(ndlMan, this);

			// this will call the callbacks
			nmp.processManifest();
		    
			nmp.freeModel();
			
		} catch (NdlException e) {
			throw new Exception(e.getMessage());
		} 
	}
	
	/**
	 * NDL-OWL Parsing
	 */
	
	public void ndlInterface(Resource l, OntModel om, Resource conn,
			Resource node, String ip, String mask) {

		if (requestPhase) {
			return;
		}
		
	}

	public void ndlCrossConnect(Resource c, OntModel m, long bw, String label,
			List<Resource> interfaces, Resource parent) {

		if (requestPhase)
			return;

		saveResourceReservation(c);

	}

	public void ndlLinkConnection(Resource c, OntModel m,
			List<Resource> interfaces, Resource parent) {
		
		if (requestPhase)
			return;
		
		saveResourceReservation(c);
			
	}

	public void ndlManifest(Resource i, OntModel m) {
		controllerUrl = NdlCommons.getManifestControllerUrl(i);
	}

	public void ndlNetworkConnection(Resource l, OntModel om, long bandwidth,
			long latency, List<Resource> interfaces) {
		if (requestPhase)
			return;
	}
	
	public void ndlNode(Resource ce, OntModel om, Resource ceClass,
			List<Resource> interfaces) {
		// will be called for both request and manifest
		if (requestPhase)
			return;
		
		// stitching nodes are special 
		if (NdlCommons.isStitchingNodeInManifest(ce)) {
			ndlCrossConnect(ce, om, 0, null, interfaces, null);
			return;
		}

		saveResourceReservation(ce);
	}
	
	@Override
	public void ndlNetworkConnectionPath(Resource c, OntModel m, List<List<Resource>> path, List<Resource> roots) {

		if (requestPhase)
			return;
		
		saveResourceReservation(c);
	}
	
	public void ndlParseComplete() {
		if (requestPhase == true)
			return;
		
		//for(Map.Entry<String, String> e: urnToReservation.entrySet()) {
		//	System.out.println("URN " + e.getKey() + " = " + e.getValue());
		//}
	}

	
	/**
	 * Lookup previously saved reservation id for a sliver urn
	 * @param sliverUrn
	 * @return
	 */
	public String getReservationId(String sliverUrn) {
		return urnToReservation.get(sliverUrn);
	}
	@Override
	public void ndlBroadcastConnection(Resource bl, OntModel om, 
			long bandwidth, List<Resource> interfaces) {
		
	}

	@Override
	public void ndlNodeDependencies(Resource ni, OntModel m,
			Set<Resource> dependencies) {
		
	}

	@Override
	public void ndlReservation(Resource i, OntModel m) {
		
		String newSliceUrn = NdlCommons.getNameProperty(i);
		if (newSliceUrn != null)
			sliceUrn = newSliceUrn;
		
		sliceUuid = NdlCommons.getGuidProperty(i);		
		sliceState = NdlCommons.getGeniSliceStateName(i);
	}

	@Override
	public void ndlReservationEnd(Literal e, OntModel m, Date end) {
		expirationTime = end;
	}

	@Override
	public void ndlReservationResources(List<Resource> r, OntModel m) {
		
	}

	@Override
	public void ndlReservationStart(Literal s, OntModel m, Date start) {
		creationTime = start;
	}

	@Override
	public void ndlReservationTermDuration(Resource d, OntModel m, int years,
			int months, int days, int hours, int minutes, int seconds) {
		if (expirationTime != null)
			return;
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
	
	@Override
	public void ndlSlice(Resource sl, OntModel m) {
		
	}
	
	private void saveResourceReservation(Resource r) {
		
		String sliverId = null;
		if (controllerUrl == null)
			sliverId = NdlToRSpecHelper.cidUrnFromUrl((NdlCommons.getDomain(r) != null ? NdlCommons.getDomain(r).getURI() : "some-domain"), 
					UrnType.Sliver, NdlToRSpecHelper.getTrueName(r));
		else
			sliverId = NdlToRSpecHelper.sliverUrnFromRack(NdlToRSpecHelper.getTrueName(r), NdlToRSpecHelper.getControllerForUrl(controllerUrl));
		String resNotice = NdlCommons.getResourceReservationNotice(r);
		
		if ( resNotice != null) 
			urnToReservation.put(sliverId, getGuidFromNotice(resNotice));
	}
	
	private static final String NOTICE_GUID_PATTERN = "^Reservation\\s+([a-zA-Z0-9-]+)\\s+.+$";
    /**
     * As a temporary measure we allow extracting guid from reservation notice /ib 08/20/14
     */
    private static Pattern noticeGuidPattern = Pattern.compile(NOTICE_GUID_PATTERN);
    private static String getGuidFromNotice(String notice) {
            if (notice == null)
                    return null;
            java.util.regex.Matcher m = noticeGuidPattern.matcher(notice.trim());
            if (m.matches()) {
                    return m.group(1);
            }
            return null;
    }
}
