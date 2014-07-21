package org.renci.pubsub_daemon.workers;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import orca.ndl.NdlToRSpecHelper;

import org.renci.pubsub_daemon.Globals;
import org.renci.pubsub_daemon.util.DbPool;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public class GENIWorker extends AbstractWorker {
	private static final int MS_TO_US = 1000;
	private static final String GENI_SELFREF_PREFIX_PROPERTY = "GENI.selfref.prefix";
	private static final String GENIWorkerName = "GENI Manifest worker; puts RSpec elements into the datastore";

	//String selfRefPrefix = "http://rci-hn.exogeni.net/info/";
	private static final String selfRefPrefix = getConfigProperty(GENI_SELFREF_PREFIX_PROPERTY);

	protected static final String COMMON_PATH = "/rspec/*/";
	protected static final String SLIVER_INFO_PATH = "geni_sliver_info";
	protected static final String COMPONENT_MANAGER = "component_manager";
	protected static final String SLIVER_TYPE = "sliver_type/@name";
	protected static final String SLIVER_SCHEMA = "http://www.gpolab.bbn.com/monitoring/schema/20140501/sliver#";

	private static final String GENIDS_URL = "GENIDS.url";
	private static final String GENIDS_USER = "GENIDS.user";
	private static final String GENIDS_PASS = "GENIDS.password";
	private static final String GENI_THREADPOOL_SIZE = "GENI.callback.size";
	private static final String GENI_LINK_CALLBACK = "GENI.callback.link";
	private static final String GENI_NODE_CALLBACK = "GENI.callback.node";
	private static final String GENI_SITE_PREFIX = "GENI.site.prefix";

	protected static DbPool conPool = null;
	protected static Boolean flag = true;

	protected String sliceUrn, sliceUuid, sliceSmName, sliceSmGuid;

	// create a static pool size that gets whacked on exit
	private static ExecutorService threadPool = null;
	{
		String poolSize = Globals.getInstance().getConfigProperty(GENI_THREADPOOL_SIZE);
		Integer pSize = 10;
		if (poolSize != null)
			pSize = Integer.decode(poolSize);
		threadPool = Executors.newFixedThreadPool(pSize);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				Globals.info("Destroying the GENIWorker thread pool");
				if (threadPool != null)
					threadPool.shutdown();
			}
		});
	}

	@Override
	public String getName() {
		return GENIWorkerName;
	}

	@Override
	public void processManifest(Map<DocType, String> manifests,
			String sliceUrn, String sliceUuid, String sliceSmName,
			String sliceSmGuid) throws RuntimeException {

		synchronized(flag) {
			if (conPool == null) 
				conPool = new DbPool(Globals.getInstance().getConfigProperty(GENIDS_URL), 
						Globals.getInstance().getConfigProperty(GENIDS_USER), 
						Globals.getInstance().getConfigProperty(GENIDS_PASS));
		}

		checkManifests(manifests);

		this.manifests = manifests;
		this.sliceUrn = sliceUrn;
		this.sliceUuid = sliceUuid;
		this.sliceSmName = sliceSmName;
		this.sliceSmGuid = sliceSmGuid;

		insertInDb();
	}

	private void executeAndClose(PreparedStatement pst) {
		executeAndClose(pst, 0);
	}

	private final static int SQL_RETRIES = 3;

	/**
	 * Guard against transient SQL errors
	 * @param pst
	 * @param tryIndex
	 */
	private void executeAndClose(PreparedStatement pst, int tryIndex) {
		try {
			pst.execute();
			pst.close();
		} catch (SQLException e) {
			if (tryIndex < SQL_RETRIES)
				executeAndClose(pst, ++tryIndex);
			else
				throw new RuntimeException("Unable to insert into the database: " + e);
		}
	}

	private void insertInDb() {
		try {
			if (selfRefPrefix == null) {
				Globals.error("selfref.prefix is not set; should be a url pointing to this datastore");
			}

			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
			Document doc = docBuilder.parse(new ByteArrayInputStream(manifests.get(DocType.RSPEC_MANIFEST).getBytes(Charset.forName("UTF-8"))));

			// normalize text representation
			doc.getDocumentElement().normalize();

			XPathFactory xPathfactory = XPathFactory.newInstance();
			XPath xpath = xPathfactory.newXPath();
			// look for nodes and links
			XPathExpression expr = xpath.compile("/rspec/node");
			NodeList nl = (NodeList)expr.evaluate(doc, XPathConstants.NODESET);
			insertSliverInfo(nl, xpath, SliverType.NODE);

			expr = xpath.compile("/rspec/link");
			nl = (NodeList)expr.evaluate(doc, XPathConstants.NODESET);
			insertSliverInfo(nl, xpath, SliverType.LINK);

		} catch (SAXParseException err) {
			throw new RuntimeException("Unable to parse document line " + err.getLineNumber () + ", uri " + err.getSystemId () + " " + err.getMessage ());
		} catch (SAXException e) {
			throw new RuntimeException("SAX exception: " + e);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Unable to parse the manifest: " + e);
		}
	}

	@Override
	public List<DocType> listDocTypes() {
		return Arrays.asList(DocType.RSPEC_MANIFEST);
	}

	// instance sizes in monitoring table are in KB
	private static final Integer GB_to_KB = 1024*1024;
	private static final Integer MB_to_KB = 1024;

	private static Map<String, Integer> instanceSizes;
	{
		Map<String, Integer> tmpMap = new HashMap<String, Integer>();

		// sizes are in kb
		tmpMap.put("raw", 48*GB_to_KB);
		tmpMap.put("rawpc", 48*GB_to_KB);
		tmpMap.put("exogeni-m4", 48*GB_to_KB);
		tmpMap.put("xo.small", GB_to_KB);
		tmpMap.put("xo.medium", GB_to_KB);
		tmpMap.put("xo.large", 2*GB_to_KB);
		tmpMap.put("xo.xlarge", 4*GB_to_KB);
		tmpMap.put("m1.small", 128*MB_to_KB);
		tmpMap.put("m1.medium", 512*MB_to_KB);
		tmpMap.put("m1.large",GB_to_KB);
		tmpMap.put("c1.medium", 256*MB_to_KB);
		tmpMap.put("c1.xlarge", 2*GB_to_KB);

		instanceSizes = Collections.unmodifiableMap(tmpMap);
	}

	// insert and run a callback
	private void insertNode(Node nl, XPath xpath, String id, String urn, String href, Date ts, Connection dbc) {
		try {
			String nodeType = xpath.compile(SLIVER_TYPE).evaluate(nl);
			Globals.info("Adding node " + urn + " of " + id + " to node table and callback");
			
			// based on type, guess the memory
			String size;
			if ((nodeType != null) && (instanceSizes.containsKey(nodeType))) {
				size = instanceSizes.get(nodeType).toString();
			} else {
				size = "unknown";
			}
			// insert into table
			if (Globals.getInstance().isDebugOn()) {
				Globals.debug("Instance size for " + nodeType + " is " + size);
			}

			if (!conPool.poolValid()) {
				Globals.error("Datastore parameters are not valid, not saving");
			} else {
				// insert into ops_node
				Globals.debug("Inserting into ops_node");
				PreparedStatement pst1 = dbc.prepareStatement("INSERT INTO `ops_node` ( `$schema` , `id` , `selfRef` , `urn` , `ts`, `properties$mem_total_kb`, " + 
						"`properties$vm_server_type` ) values (?, ?, ?, ?, ?, ?, ?)");
				pst1.setString(1, SLIVER_SCHEMA);
				pst1.setString(2, id);
				pst1.setString(3, href);
				pst1.setString(4, urn);
				pst1.setLong(5, ts.getTime()*MS_TO_US);
				pst1.setString(6, size);
				pst1.setString(7, nodeType);
				executeAndClose(pst1);
			}
			
			URI pUrl = null;
			try {
				String tmpProp = Globals.getInstance().getConfigProperty(GENI_NODE_CALLBACK);
				if (tmpProp != null)
					tmpProp = tmpProp.trim();
				pUrl = new URI(tmpProp);
			} catch (URISyntaxException e) {
				Globals.error("Error publishing to invalid URL: " + Globals.getInstance().getConfigProperty(GENI_NODE_CALLBACK));
				return;
			} catch (NullPointerException ne) {
				;
			}

			// exec 
			if ((pUrl != null) && ("exec".equals(pUrl.getScheme()))) {
				// run through an executable

				Globals.info("Running through node callback " + pUrl.getPath());
				final ArrayList<String> myCommand = new ArrayList<String>();

				myCommand.add(pUrl.getPath());
				myCommand.add(nodeType);
				myCommand.add(id);
				myCommand.add(urn);
				myCommand.add(href);

				threadPool.submit(new Runnable() {
					@Override
					public void run() {
						Globals.executeCommand(myCommand, null);
					}
				});
			} else {
				Globals.error("Node callback invalid or not specified: " + (pUrl != null ? pUrl.toString() : "null"));
			}
		} catch (XPathExpressionException xe) {
			throw new RuntimeException("XPath exception: " + xe);
		} catch(SQLException se) {
			throw new RuntimeException("SQL exception: " + se);
		}
	}

	// insert and run a callback
	private void insertLink(Node nl, XPath xpath, String id, String urn, String href, Date ts, Connection dbc) {
		try {
			Globals.info("Adding link " + urn + " of vlan " + id + " to link table and callback");
			if (!conPool.poolValid()) {
				Globals.error("Datastore parameters are not valid, not saving to ops_link");
			} else {
				// insert into ops_link
				Globals.debug("Inserting into ops_link");
				PreparedStatement pst1 = dbc.prepareStatement("INSERT INTO `ops_link` ( `$schema` , `id` , `selfRef` , `urn` , `ts` )" + 
						" values (?, ?, ?, ?, ?)");
				pst1.setString(1, SLIVER_SCHEMA);
				pst1.setString(2, id);
				pst1.setString(3, href);
				pst1.setString(4, urn);
				pst1.setLong(5, ts.getTime()*MS_TO_US);
				executeAndClose(pst1);
			}
			URI pUrl = null;
			try {
				String tmpProp = Globals.getInstance().getConfigProperty(GENI_LINK_CALLBACK);
				if (tmpProp != null)
					tmpProp = tmpProp.trim();
				pUrl = new URI(tmpProp);
			} catch (URISyntaxException e) {
				Globals.error("Error publishing to invalid URL: " + Globals.getInstance().getConfigProperty(GENI_LINK_CALLBACK));
				return;
			} catch (NullPointerException ne) {
				;
			}

			// exec 
			if ((pUrl != null) && ("exec".equals(pUrl.getScheme()))) {
				// run through an executable

				Globals.info("Running through link callback " + pUrl.getPath());
				final ArrayList<String> myCommand = new ArrayList<String>();

				myCommand.add(pUrl.getPath());
				myCommand.add(id);
				myCommand.add(urn);
				myCommand.add(href);

				threadPool.submit(new Runnable() {
					@Override
					public void run() {
						Globals.executeCommand(myCommand, null);
					}
				});
			} else {
				Globals.error("Link callback invalid or not specified: " + (pUrl != null ? pUrl.toString() : "null"));
			}
			
		} catch(SQLException se) {
			throw new RuntimeException("SQL exception: " + se);
		}
	}

	private enum SliverType {NODE, LINK};

	private void insertSliverInfo(NodeList nl, XPath xpath, SliverType t) {
		// insert into datastore
		Connection dbc = null;
		try {
			// get sliver information
			Globals.debug("There are " + nl.getLength() + " elements of type " + t.name());
			String shortName = Globals.getInstance().getConfigProperty(GENI_SITE_PREFIX);
			if (shortName == null) {
				Globals.warn("No short site prefix GENI.site.prefix specified in the configuration, no slivers will be inserted in the databse");
				return;
			}
			for (int i = 0; i < nl.getLength(); i++) {

				String type = nl.item(i).getNodeName();

				URI sliver_urn = new URI(xpath.compile("@sliver_id").evaluate(nl.item(i)));

				String sliver_id = sliver_urn.toString().replaceFirst("urn:publicid:IDN\\+", "").replaceAll("[+:]", "_");
				String sliver_href = selfRefPrefix + "sliver/" + sliver_id;
				String sliver_uuid = sliver_urn.toString().replaceFirst("urn.+sliver\\+", "").split(":")[0];

				Date ts = new Date();

				// this works for nodes.
				NodeList cmAttr = (NodeList)xpath.compile("@component_manager_id").evaluate(nl.item(i), XPathConstants.NODESET);
				String cm = null;
				
				if (cmAttr.getLength() > 0) {
					// Get component manager name from the attribute
					cm = xpath.compile("@component_manager_id").evaluate(nl.item(i));
					if (!cm.equalsIgnoreCase("urn:publicid:IDN+exogeni.net:" + shortName + "vmsite+authority+am")) {
						cm = null;
					}
				} else {
					// iterate over component managers and find one we're looking for (if there is one)
					NodeList cmElem = (NodeList)xpath.compile(COMPONENT_MANAGER).evaluate(nl.item(i), XPathConstants.NODESET);
					for (int jj = 0; jj < cmElem.getLength(); jj++) {
						String tcm = xpath.compile("@name").evaluate(cmElem.item(jj));
						if (tcm.equalsIgnoreCase("urn:publicid:IDN+exogeni.net:" + shortName + "vmsite+authority+am") || 
								tcm.equalsIgnoreCase("urn:publicid:IDN+exogeni.net:" + shortName + "Net+authority+am")) {
							cm = tcm;
							break;
						}
					}
					
				}
				
				if (cm == null) {
					Globals.warn("Unable to determine component manager for " + sliver_id + " skipping reporting");
					continue;
				}
				String[] siteId = cm.split("\\+");
				// fully qualified aggregate id (e.g. exogeni.net:bbnvmsite)
				String full_agg_id = siteId[Math.min(siteId.length - 1, 1)];
				String[] globalComp = siteId[Math.min(siteId.length - 1 , 1)].split(":");
				// short aggregate id (e.g. bbnvmsite)
				String agg_id = globalComp[Math.min(globalComp.length - 1, 1)];
				String aggregate_href = selfRefPrefix + "aggregate/" + agg_id;
				URI aggregate_urn = new URI(NdlToRSpecHelper.CM_URN_PATTERN.replaceAll("@", agg_id));
				
				// find geni_sliver_info, if available
				if (((NodeList)xpath.compile(SLIVER_INFO_PATH).evaluate(nl.item(i), XPathConstants.NODESET)).getLength() > 0) {
					String creator = xpath.compile(SLIVER_INFO_PATH + "/@creator_urn").evaluate(nl.item(i));

					URI creator_urn = null;
					try {
						if ((creator != null) && (creator.split(",").length == 2))
							creator_urn = new URI(creator.split(",")[1].trim());
						else { 
							if ((creator != null) && (creator.length() >0)) 
								creator_urn = new URI(dnToUrn(creator));
							else
								creator_urn = new URI("urn:publicid:IDN+unknownuser");
						}
					} catch (URISyntaxException ue) {
						Globals.error("Unable to parse creator string " + creator + ", replacing with dummy value");
						creator_urn = new URI("urn:publicid:IDN+unknownuser");
					}

					String created = xpath.compile(SLIVER_INFO_PATH + "/@start_time").evaluate(nl.item(i));
					Calendar cal = javax.xml.bind.DatatypeConverter.parseDateTime(created);
					Date createdDate = cal.getTime();
					String expires = xpath.compile(SLIVER_INFO_PATH + "/@expiration_time").evaluate(nl.item(i));
					cal = javax.xml.bind.DatatypeConverter.parseDateTime(expires);
					Date expiresDate = cal.getTime();

					String resource = xpath.compile(SLIVER_INFO_PATH + "/@resource_id").evaluate(nl.item(i));
					if ((resource == null) || (resource.length() == 0)) {
						continue;
					}

					String resource_urn = NdlToRSpecHelper.SLIVER_URN_PATTERN.replaceAll("@", full_agg_id).replaceAll("\\^", type).replaceAll("%", resource);
					String resource_href = selfRefPrefix + "resource/" + resource;

					if (!Globals.getInstance().isDebugOn()) {
						Globals.debug("Slice: " + sliceUrn + " uuid: " + sliceUuid);
						Globals.debug("Sliver: " + type + " " + sliver_id + " " + sliver_uuid + " " + sliver_href);
						Globals.debug("URN of " + type + ": "+ sliver_urn);
						Globals.debug("Aggregate URN: " + aggregate_urn + " id: " + agg_id + " href:" + aggregate_href);
						Globals.debug("TS: " + ts.getTime());
						Globals.debug("Creator URN: " + creator_urn);
						Globals.debug("Created: " + createdDate.getTime() + " expires: " + expiresDate.getTime());
						Globals.debug("Resource URN: " + resource_urn);
						Globals.debug("Resource: " + resource);
					}

					if (!conPool.poolValid()) {
						Globals.error("Datastore parameters are not valid, not saving");
					} else {
						dbc = conPool.getDbConnection();
						// insert into ops_sliver
						Globals.debug("Inserting into ops_sliver");
						PreparedStatement pst1 = dbc.prepareStatement("INSERT INTO `ops_sliver` ( `$schema` , `id` , `selfRef` , `urn` , `uuid`, `ts`, `aggregate_urn`, " + 
								"`aggregate_href` , `slice_urn` , `slice_uuid` , `creator` , `created` , `expires`) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
						pst1.setString(1, SLIVER_SCHEMA);
						pst1.setString(2, sliver_id);
						pst1.setString(3, sliver_href);
						pst1.setString(4, sliver_urn.toString());
						pst1.setString(5, sliver_uuid);
						pst1.setLong(6, ts.getTime()*MS_TO_US);
						pst1.setString(7, aggregate_urn.toString());
						pst1.setString(8, aggregate_href);
						pst1.setString(9, sliceUrn);
						pst1.setString(10, sliceUuid);
						pst1.setString(11, creator_urn.toString());
						pst1.setLong(12, createdDate.getTime()*MS_TO_US);
						pst1.setLong(13, expiresDate.getTime()*MS_TO_US);
						executeAndClose(pst1);

						// insert into ops_aggregate_sliver
						Globals.debug("Inserting into ops_aggregate_sliver");
						PreparedStatement pst2 = dbc.prepareStatement("INSERT INTO `ops_aggregate_sliver` ( `id` , `aggregate_id`, `urn` , `selfRef`) values (?, ?, ?, ?)");
						pst2.setString(1, sliver_id);
						pst2.setString(2, agg_id);
						pst2.setString(3, aggregate_urn.toString());
						pst2.setString(4, aggregate_href);
						executeAndClose(pst2);

						// insert into ops_sliver_resource
						Globals.debug("Inserting into ops_sliver_resource");
						PreparedStatement pst3 = dbc.prepareStatement("INSERT INTO `ops_sliver_resource` ( `id` , `sliver_id` , `urn` , `selfRef` ) values (?, ?, ?, ?)");
						pst3.setString(1, resource);
						pst3.setString(2, sliver_id);
						pst3.setString(3, resource_urn);
						pst3.setString(4, resource_href);
						executeAndClose(pst3);
					}

					switch(t) {
					case NODE:
						insertNode(nl.item(i), xpath, resource, resource_urn, resource_href, ts, dbc);
						break;
					case LINK:
						insertLink(nl.item(i), xpath, resource, resource_urn, resource_href, ts, dbc);
						break;
					}
				} else 
					Globals.error("Unable to find sliver_info in node " + nl.item(i));
			}
		} catch(SQLException se) {
			throw new RuntimeException("Unable to insert into the database: " + se);
		} catch(XPathExpressionException xe) {
			throw new RuntimeException("Unable to parse XML manifest: " + xe);
		} catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Unable to parse manifest: " + e);
		} finally {
			if (dbc != null)
				try {
					dbc.close();
				} catch (SQLException e) {
					throw new RuntimeException("Error closing mysql db connection: " + e);
				}
		}
	}

	/**
	 * Convert a DN into a URN (mostly for BEN credentials)
	 * urn:publicid:IDN+ch.geni.net+user+ekishore
	 * @param s
	 * @return
	 * @throws RuntimeException
	 */
	private static String dnToUrn(String s) throws RuntimeException {
		if ((s != null) && (s.length() > 0)) {
			// see if this is a CN?
			try {
				LdapName ln = new LdapName(s);
				Enumeration<String> all = ln.getAll();
				StringBuilder sb = new StringBuilder();
				sb.append("urn:publicid:IDN+");
				while(all.hasMoreElements()) {
					String[] a = all.nextElement().split("=");
					if ("O".equals(a[0])) {
						a[1].replaceAll("[^a-zA-Z_0-9]", "");
						sb.append(a[1].replaceAll("[^a-zA-Z_0-9]", "") + "+"); 
					}
					if ("CN".equals(a[0])) {
						sb.append("user+" + a[1]);
					}
				}
				return sb.toString();
			} catch (InvalidNameException ie) {
				throw new RuntimeException("String " + s + " not a DN");
			}
		} else {
			throw new RuntimeException("String is empty");
		}
	}
	
	public static void main(String[] argv) {
		GENIWorker gw = new GENIWorker();
		
		try {
			InputStream source = new FileInputStream(new File("/Users/ibaldin/Desktop/two-node-manifest-intra.xml"));
			String text = new Scanner( source ).useDelimiter("\\A").next();

			gw.manifests = new HashMap<DocType, String>();
			gw.manifests.put(DocType.RSPEC_MANIFEST, text);
			gw.processManifest(gw.manifests, "URN:slice", "slice-guid", "slice-sm", "slice-sm-guid");
		} catch(Exception e) {
			System.err.println("Something went bad: " + e);
			e.printStackTrace();
		}
	}
}
