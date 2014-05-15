package org.renci.pubsub_daemon.workers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.renci.pubsub_daemon.Globals;

public class XODbWorker implements ICompressedManifestWorker {
	private static final String XODbWorkerName = "XODbWorker, puts manifests into the xo database";
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
	String manifest, compManifest, sliceUrn, sliceUuid, sliceSmName, sliceSmGuid;
	
	@Override
	public void processManifest(String manifest, String compManifest, String sliceUrn,
			String sliceUuid, String sliceSmName, String sliceSmGuid)
			throws RuntimeException {
		this.manifest = manifest;
		this.compManifest = compManifest;
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
			if (Globals.getInstance().isDbValid()) {
				Globals.info("Saving slice " + sliceUrn + " to the database " + Globals.getInstance().getDbUrl());
				// parse the manifest
				NDLManifestParser parser = new NDLManifestParser(sliceUrn, manifest);
				parser.parseAll();

				Globals.debug("Slice meta information: " + parser.getCreatorUrn() + " " + parser.getSliceUuid() + " " + parser.getSliceUrn() + " " + parser.getSliceState());
				// insert into db or update db
				dbc = Globals.getInstance().getDbConnection();
				PreparedStatement pst = dbc.prepareStatement("SELECT slice_guid FROM `xoslices` WHERE slice_guid=? AND slice_sm=?");
				pst.setString(1, parser.getSliceUuid());
				pst.setString(2, sliceSmName);
				ResultSet rs = pst.executeQuery();
				if(rs.next()) {
					// update the row
					Globals.debug("Updating row for slice " + parser.getSliceUrn() + " / " + parser.getSliceUuid());
					PreparedStatement pst1 = dbc.prepareStatement("UPDATE `xoslices` SET slice_manifest=? WHERE slice_guid=? AND slice_sm=?");
					pst1.setString(1, compManifest);
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
					pst1.setString(4, compManifest);
					pst1.setString(5, ManifestTypes.GZIPPED_ENCODED_NDL.name);
					pst1.setString(6, sliceSmName);
					pst1.execute();
					pst1.close();
				}
				rs.close();
				pst.close();
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

}
