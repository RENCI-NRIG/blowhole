package org.renci.pubsub_daemon.workers;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.renci.pubsub_daemon.Globals;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;

public class XODbWorker extends AbstractWorker {
	private static final String XODbWorkerName = "XODbWorker, puts manifests into the xo database";

	private static final String DB_URL = "DB.url";
	private static final String DB_USER = "DB.user";
	private static final String DB_PASS = "DB.password";
	
	private String dbUrl = null;
	private boolean dbValid = false;
	private ComboPooledDataSource cpds = null;
	
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
	protected Map<DocType, String> manifests;

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
		
		setDbParams(Globals.getInstance().getConfigProperty(DB_URL), 
				Globals.getInstance().getConfigProperty(DB_USER), 
				Globals.getInstance().getConfigProperty(DB_PASS));
		
		insertInDb();
	}
	
	/**
	 * Insert the compressed version of the manifest into db. Do minimal parsing of the manifest.
	 */
	private void insertInDb() throws RuntimeException {
		Connection dbc = null;
		try {
			if (isDbValid()) {
				Globals.info("Saving slice " + sliceUrn + " to the database " + dbUrl);
				// parse the manifest
				NDLManifestParser parser = new NDLManifestParser(sliceUrn, manifests.get(DocType.NDL_MANIFEST));
				parser.parseAll();

				Globals.debug("Slice meta information: " + parser.getCreatorUrn() + " " + parser.getSliceUuid() + " " + parser.getSliceUrn() + " " + parser.getSliceState());
				// insert into db or update db
				dbc = getDbConnection();
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
					pst1.execute();
					pst1.close();
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
					pst1.execute();
					pst1.close();
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

	protected void setDbParams(String url, String user, String pass) {
		dbUrl = url;
		
		// if all non-null, create a pooled connection source to the db
		if ((url == null) || (user == null) || (pass == null)) {
			dbValid = false;
			if (cpds != null) {
				try {
					DataSources.destroy(cpds);
				} catch (SQLException se) {
					Globals.error("SQL Error (non fatal): " + se);
				}
				cpds = null;
			}
			Globals.info("Insufficient database parameters, not creating a connection");
			return;
		}
		
		cpds = new ComboPooledDataSource();
		try {
			cpds.setDriverClass("com.mysql.jdbc.Driver");
		} catch (PropertyVetoException e) {
			Globals.error("Unable to create JDBC MySQL connection (non-fatal): " + e);
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
	protected Connection getDbConnection() throws SQLException {
		if (dbValid)
			return cpds.getConnection();
		throw new SQLException("Invalid database parameters");
	}
	
	protected boolean isDbValid() {
		return dbValid;
	}
}
