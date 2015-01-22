package org.renci.pubsub_daemon.workers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.renci.pubsub_daemon.Globals;
import org.renci.pubsub_daemon.util.DbPool;

public class XODbWorker extends AbstractWorker {
	private static final String XODbWorkerName = "XODbWorker, puts manifests into the xo database";

	private static final String DB_URL = "XODB.url";
	private static final String DB_USER = "XODB.user";
	private static final String DB_PASS = "XODB.password";

	protected static DbPool conPool = null;
	protected static Boolean flag = true;


	public static enum ManifestTypes {
		GZIPPED_ENCODED_NDL("NDL GZipped/Base-64 encoded"),
		NDL_XML("RDF-XML NDL");

		String name;
		ManifestTypes(String n) {
			name = n;
		}

		public String getLongName() {
			return name;
		}
	}
	protected String sliceUrn, sliceUuid, sliceSmName, sliceSmGuid;

	@Override
	public void processManifest(Map<DocType, String> manifests,
			String sliceUrn, String sliceUuid, String sliceSmName,
			String sliceSmGuid) throws RuntimeException {

		synchronized(flag) {
			if (conPool == null)
				conPool = new DbPool(Globals.getInstance().getConfigProperty(DB_URL), 
						Globals.getInstance().getConfigProperty(DB_USER), 
						Globals.getInstance().getConfigProperty(DB_PASS));
		}

		checkManifests(manifests);

		this.manifests = manifests;
		this.sliceUrn = sliceUrn;
		this.sliceUuid = sliceUuid;
		this.sliceSmName = sliceSmName;
		this.sliceSmGuid = sliceSmGuid;

		insertInDb();
	}

	/**
	 * Insert the compressed version of the manifest into db. Do minimal parsing of the manifest.
	 */
	private void insertInDb() throws RuntimeException {
		Connection dbc = null;
		try {
			if (conPool.poolValid()) {
				Globals.info("Saving slice " + sliceUrn + " to the database " + conPool.getUrl());
				// parse the manifest
				NDLManifestParser parser = new NDLManifestParser(sliceUrn, manifests.get(DocType.NDL_MANIFEST));
				parser.parseAll();

				Globals.debug("Slice meta information: " + parser.getCreatorUrn() + " " + parser.getSliceUuid() + " " + parser.getSliceUrn() + " " + parser.getSliceState());
				// insert into db or update db
				dbc = conPool.getDbConnection();
				PreparedStatement pst = dbc.prepareStatement("SELECT slice_guid FROM `xoslices` WHERE slice_guid=? AND slice_sm=?");
				pst.setString(1, parser.getSliceUuid());
				pst.setString(2, sliceSmName);
				ResultSet rs = pst.executeQuery();
				if(rs.next()) {
					// update the row
					Globals.debug("Updating row for slice " + parser.getSliceUrn() + " / " + parser.getSliceUuid());
					PreparedStatement pst1 = dbc.prepareStatement("UPDATE `xoslices` SET slice_manifest=? WHERE slice_guid=? AND slice_sm=?");
					pst1.setString(1, manifests.get(DocType.COMPRESSED_NDL_MANIFEST));
					pst1.setString(2, parser.getSliceUuid());
					pst1.setString(3, sliceSmName);
					executeAndClose(pst1);
				} else {
					// insert new row
					Globals.debug("Inserting new row for slice " + parser.getSliceUrn() + " / " + parser.getSliceUuid());
					PreparedStatement pst1 = dbc.prepareStatement("INSERT into `xoslices` ( `slice_name` , `slice_guid` , `slice_owner`, `slice_manifest`, " + 
							"`slice_manifest_type`, `slice_sm`) values (?, ?, ?, ?, ?, ?)");
					pst1.setString(1, parser.getSliceUrn());
					pst1.setString(2, parser.getSliceUuid());
					pst1.setString(3, parser.getCreatorUrn());
					pst1.setString(4, manifests.get(DocType.COMPRESSED_NDL_MANIFEST));
					pst1.setString(5, ManifestTypes.GZIPPED_ENCODED_NDL.name);
					pst1.setString(6, sliceSmName);
					executeAndClose(pst1);
				}
				rs.close();
				pst.close();
			} else {
				Globals.error("Unable to save slice " + sliceUrn + " to the database due to insufficient/invalid db parameters");
			}
		} catch (SQLException e) {
			throw new RuntimeException("Unable to insert into the database: " + e);
		} catch (Exception e) {
			throw new RuntimeException("Unable to parse the manifest: " + e);
		} finally {
			if (dbc != null)
				try {
					dbc.close();
				} catch (SQLException e) {
					throw new RuntimeException("Error closing connection: " + e);
				}
		}
	}

	@Override
	public String getName() {
		return XODbWorkerName;
	}

	@Override
	public List<DocType> listDocTypes() {
		return Arrays.asList(AbstractWorker.DocType.COMPRESSED_NDL_MANIFEST, AbstractWorker.DocType.NDL_MANIFEST);
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
	
	public void runAtStartup() {
		
	}
}
