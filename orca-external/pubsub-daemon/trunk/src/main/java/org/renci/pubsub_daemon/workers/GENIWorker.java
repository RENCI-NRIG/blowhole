package org.renci.pubsub_daemon.workers;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import orca.ndl.NdlToRSpecHelper;

import org.renci.pubsub_daemon.Globals;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public class GENIWorker extends XODbWorker {
	public static final String GENIWorkerName = "GENI Manifest worker; puts RSpec elements into the datastore";
	public static final String COMMON_PATH = "/rspec/*/";
	public static final String SLIVER_INFO_PATH = "geni_sliver_info";
	public static final String SLIVER_SCHEMA = "http://www.gpolab.bbn.com/monitoring/schema/20140501/sliver#";

	public static final String GENIDS_URL = "GENIDS.url";
	public static final String GENIDS_USER = "GENIDS.user";
	public static final String GENIDS_PASS = "GENIDS.password";
	public static Semaphore sem = new Semaphore(1);

	@Override
	public String getName() {
		return GENIWorkerName;
	}

	@Override
	public void processManifest(Map<DocType, String> manifests,
			String sliceUrn, String sliceUuid, String sliceSmName,
			String sliceSmGuid) throws RuntimeException {

		checkManifests(manifests);

		this.manifests = manifests;
		this.sliceUrn = sliceUrn;
		this.sliceUuid = sliceUuid;
		this.sliceSmName = sliceSmName;
		this.sliceSmGuid = sliceSmGuid;

		setDbParams(Globals.getInstance().getConfigProperty(GENIDS_URL), 
				Globals.getInstance().getConfigProperty(GENIDS_USER), 
				Globals.getInstance().getConfigProperty(GENIDS_PASS));

		insertInDb();
	}

	@SuppressWarnings("restriction")
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
			String selfRefPrefix = getConfigProperty("selfref.prefix");

			if (selfRefPrefix == null) {
				Globals.error("selfref.prefix is not set; should be a url pointing to this datastore");
				selfRefPrefix = "please set selfref.prefix";
			}

			// get sliver information
			for (int i = 0; i < nl.getLength(); i++) {
				String type = nl.item(i).getNodeName();
				// Skipping links for now
				if ("link".equals(type.trim()))
					continue;

				URI sliver_urn = new URI(xpath.compile("@sliver_id").evaluate(nl.item(i)));

				String sliver_id = sliver_urn.toString().replaceFirst("urn:publicid:IDN\\+", "").replaceAll("[+:]", "_");
				String sliver_href = selfRefPrefix + "sliver/" + sliver_id;
				String sliver_uuid = sliver_urn.toString().replaceFirst("urn.+sliver\\+", "").split(":")[0];

				Date ts = new Date();

				URI aggregate_urn = new URI(xpath.compile("@component_manager_id").evaluate(nl.item(i)));
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
					if ((creator != null) && (creator.split(",").length == 2))
						creator_urn = new URI(creator.split(",")[1].trim());

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

					sem.acquire();
					if (Globals.getInstance().isDebugOn()) {
						Globals.info("Slice: " + sliceUrn + " uuid: " + sliceUuid);
						Globals.info("Sliver: " + type + " " + sliver_id + " " + sliver_uuid + " " + sliver_href);
						Globals.info("URN of " + type + ": "+ sliver_urn);
						Globals.info("Aggregate URN: " + aggregate_urn + " id: " + agg_id + " href:" + aggregate_href);
						Globals.info("TS: " + ts.getTime());
						Globals.info("Creator URN: " + creator_urn);
						Globals.info("Created: " + createdDate.getTime() + " expires: " + expiresDate.getTime());
						Globals.info("Resource URN: " + resource_urn);
					}
					sem.release();

					if (!isDbValid()) 
						continue;
						
					// insert into datastore
					dbc = getDbConnection();

					// insert into ops_sliver
					Globals.debug("Inserting into ops_sliver");
					PreparedStatement pst1 = dbc.prepareStatement("INSERT INTO `ops_sliver` ( `$schema` , `id` , `selfRef` , `urn` , `uuid`, `ts`, `aggregate_urn`, " + 
							"`aggregate_href` , `slice_urn` , `slice_uuid` , `creator` , `created` , `expires`) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
					pst1.setString(1, SLIVER_SCHEMA);
					pst1.setString(2, sliver_id);
					pst1.setString(3,  sliver_href);
					pst1.setString(4, sliver_urn.toString());
					pst1.setString(5, sliver_uuid);
					pst1.setLong(6, ts.getTime());
					pst1.setString(7, aggregate_urn.toString());
					pst1.setString(8, aggregate_href);
					pst1.setString(9, sliceUrn);
					pst1.setString(10, sliceUuid);
					pst1.setString(11, creator_urn.toString());
					pst1.setLong(12, createdDate.getTime());
					pst1.setLong(13, expiresDate.getTime());
					pst1.execute();
					pst1.close();

					// insert into ops_aggregate_sliver
					Globals.debug("Inserting into ops_aggregate_sliver");
					PreparedStatement pst2 = dbc.prepareStatement("INSERT INTO `ops_aggregate_sliver` ( `id` , `aggregate_id`, `urn` , `selfRef`) values (?, ?, ?, ?)");
					pst2.setString(1, sliver_id);
					pst2.setString(2, agg_id);
					pst2.setString(3, aggregate_urn.toString());
					pst2.setString(4, aggregate_href);
					pst2.execute();
					pst2.close();

					// insert into ops_sliver_resource
					Globals.debug("Inserting into ops_sliver_resource");
					PreparedStatement pst3 = dbc.prepareStatement("INSERT INTO `ops_sliver_resource` ( `id` , `sliver_id` , `urn` , `selfRef` ) values (?, ?, ?, ?)");
					pst3.setString(1, resource);
					pst3.setString(2, sliver_id);
					pst3.setString(3, resource_urn.toString());
					pst3.setString(4, resource_href);
					pst3.execute();
					pst3.close();

				}
			}
		} catch (SAXParseException err) {
			throw new RuntimeException("Unable to parse document line " + err.getLineNumber () + ", uri " + err.getSystemId () + " " + err.getMessage ());
		} catch (SAXException e) {
			throw new RuntimeException("SAX exception: " + e);
		} catch (SQLException e) {
			throw new RuntimeException("Unable to insert into the database: " + e);
		} catch (Exception e) {
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

	@SuppressWarnings("restriction")
	public static void main(String[] argv) {
		GENIWorker gw = new GENIWorker();

		//String rspec = Globals.readFileToString(argv[0]);
		//Map<DocType, String> m = new HashMap<DocType, String>();
		//System.out.println("Expected types: " + gw.listDocTypes());
		//m.put(DocType.RSPEC_MANIFEST, rspec);		
		//gw.processManifest(m, null, null, null, null);

		Calendar cal = javax.xml.bind.DatatypeConverter.parseDateTime("2010-01-01T12:00:00Z");
		Calendar cal1 = javax.xml.bind.DatatypeConverter.parseDateTime("2014-05-19T12:07:36.763-05:00");

		System.out.println(cal.getTime());

		System.out.println(cal1.getTime());

		String component_manager_id = "urn:publicid:IDN+exogeni.net:bbnvmsite+authority+am";

		String[] siteId = component_manager_id.split("\\+");
		String[] globalComp = siteId[Math.min(siteId.length - 1 , 1)].split(":");
		String aggId = globalComp[Math.min(globalComp.length - 1, 1)];

		System.out.println("Aggregate id " +aggId );

	}
}
