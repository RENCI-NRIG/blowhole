package org.renci.pubsub_daemon.workers;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import orca.ndl.NdlToRSpecHelper;

import org.renci.pubsub_daemon.Globals;
import org.renci.pubsub_daemon.util.DbPool;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public class GENIWorker extends AbstractWorker {
	private static final int MS_TO_US = 1000;
	private static final String GENI_SELFREF_PREFIX_PROPERTY = "GENI.selfref.prefix";
	private static final String GENIWorkerName = "GENI Manifest worker; puts RSpec elements into the datastore";
	protected static final String COMMON_PATH = "/rspec/*/";
	protected static final String SLIVER_INFO_PATH = "geni_sliver_info";
	protected static final String SLIVER_SCHEMA = "http://www.gpolab.bbn.com/monitoring/schema/20140501/sliver#";

	private static final String GENIDS_URL = "GENIDS.url";
	private static final String GENIDS_USER = "GENIDS.user";
	private static final String GENIDS_PASS = "GENIDS.password";

	protected static DbPool conPool = null;
	protected static Boolean flag = true;

	protected String sliceUrn, sliceUuid, sliceSmName, sliceSmGuid;

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
		Connection dbc = null;
		try {
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
			Document doc = docBuilder.parse(new ByteArrayInputStream(manifests.get(DocType.RSPEC_MANIFEST).getBytes(Charset.forName("UTF-8"))));

			// normalize text representation
			doc.getDocumentElement().normalize();


			XPathFactory xPathfactory = XPathFactory.newInstance();
			XPath xpath = xPathfactory.newXPath();
			// look for nodes and links
			XPathExpression expr = xpath.compile("/rspec/node | /rspec/link");
			NodeList nl = (NodeList)expr.evaluate(doc, XPathConstants.NODESET);

			//String selfRefPrefix = "http://rci-hn.exogeni.net/info/";
			String selfRefPrefix = getConfigProperty(GENI_SELFREF_PREFIX_PROPERTY);

			if (selfRefPrefix == null) {
				Globals.error("selfref.prefix is not set; should be a url pointing to this datastore");
				selfRefPrefix = "please set selfref.prefix ";
			}

			// get sliver information
			for (int i = 0; i < nl.getLength(); i++) {
				String type = nl.item(i).getNodeName();

				URI sliver_urn = new URI(xpath.compile("@sliver_id").evaluate(nl.item(i)));

				String sliver_id = sliver_urn.toString().replaceFirst("urn:publicid:IDN\\+", "").replaceAll("[+:]", "_");
				String sliver_href = selfRefPrefix + "sliver/" + sliver_id;
				String sliver_uuid = sliver_urn.toString().replaceFirst("urn.+sliver\\+", "").split(":")[0];

				Date ts = new Date();

				// we get it from sliver_id because it is consistent for nodes and links
				// links don't have a single component manager id, so its a pain. /ib 05/22/14
				URI aggregate_urn = new URI(xpath.compile("@sliver_id").evaluate(nl.item(i)));
				String[] siteId = aggregate_urn.toString().split("\\+");
				// fully qualified aggregate id (e.g. exogeni.net:bbnvmsite)
				String full_agg_id = siteId[Math.min(siteId.length - 1, 1)];
				String[] globalComp = siteId[Math.min(siteId.length - 1 , 1)].split(":");
				// short aggregate id (e.g. bbnvmsite)
				String agg_id = globalComp[Math.min(globalComp.length - 1, 1)];
				String aggregate_href = selfRefPrefix + "aggregate/" + agg_id;

				if (xpath.compile(SLIVER_INFO_PATH).evaluate(nl.item(i)) != null) {
					String creator = xpath.compile(SLIVER_INFO_PATH + "/@creator_urn").evaluate(nl.item(i));
					
					URI creator_urn = null;
					try {
						if ((creator != null) && (creator.split(",").length == 2))
							creator_urn = new URI(creator.split(",")[1].trim());
						else { 
							creator_urn = new URI(dnToUrn(creator));
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

					if (Globals.getInstance().isDebugOn()) {
						Globals.debug("Slice: " + sliceUrn + " uuid: " + sliceUuid);
						Globals.debug("Sliver: " + type + " " + sliver_id + " " + sliver_uuid + " " + sliver_href);
						Globals.debug("URN of " + type + ": "+ sliver_urn);
						Globals.debug("Aggregate URN: " + aggregate_urn + " id: " + agg_id + " href:" + aggregate_href);
						Globals.debug("TS: " + ts.getTime());
						Globals.debug("Creator URN: " + creator_urn);
						Globals.debug("Created: " + createdDate.getTime() + " expires: " + expiresDate.getTime());
						Globals.debug("Resource URN: " + resource_urn);
					}

					if (!conPool.poolValid()) {
						Globals.error("Datastore parameters are not valid, not saving");
						continue;
					}

					// insert into datastore
					dbc = conPool.getDbConnection();

					// insert into ops_sliver
					Globals.debug("Inserting into ops_sliver");
					PreparedStatement pst1 = dbc.prepareStatement("INSERT INTO `ops_sliver` ( `$schema` , `id` , `selfRef` , `urn` , `uuid`, `ts`, `aggregate_urn`, " + 
							"`aggregate_href` , `slice_urn` , `slice_uuid` , `creator` , `created` , `expires`) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
					pst1.setString(1, SLIVER_SCHEMA);
					pst1.setString(2, sliver_id);
					pst1.setString(3,  sliver_href);
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
					pst3.setString(3, resource_urn.toString());
					pst3.setString(4, resource_href);
					executeAndClose(pst3);
				}
			}
		} catch (SAXParseException err) {
			throw new RuntimeException("Unable to parse document line " + err.getLineNumber () + ", uri " + err.getSystemId () + " " + err.getMessage ());
		} catch (SAXException e) {
			throw new RuntimeException("SAX exception: " + e);
		} catch (SQLException e) {
			throw new RuntimeException("Unable to insert into the database: " + e);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Unable to parse the manifest: " + e);
		} finally {
			if (dbc != null)
				try {
					dbc.close();
				} catch (SQLException e) {
					throw new RuntimeException("Error closing mysql db connection: " + e);
				}
		}
	}

	@Override
	public List<DocType> listDocTypes() {
		return Arrays.asList(DocType.RSPEC_MANIFEST);
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
}
