package org.renci.pubsub_daemon;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.DataFormatException;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;

/**
 * This thread takes a base-64-encoded and gzipped manifest,
 * uncompresses it, decodes via remote XMLRPC-NDL converter
 * function and publishes it to a desired URL.
 * 
 * It also puts manifests into the database
 * 
 * @author ibaldin
 *
 */
public class ManifestWorkerThread implements Runnable {
	private static final String MANIFEST_TYPE_GZIPPED_BASE_64_ENCODED_NDL = "NDL GZipped/Base-64 encoded";
	private static final String MANIFEST_TO_RSPEC = "ndlConverter.manifestToRSpec3";
	private final String man;
	private final String sliceUrn;
	private final String sliceUuid;
	private final String sliceSmName;
	private final String sliceSmGuid;

	ManifestWorkerThread(String manifest, String sliceUrn, String sliceUuid, String sliceSmName, String sliceSmGuid) {
		Globals.debug("Worker thread starting for slice "  + sliceUrn + " from " + sliceSmName);
		man = manifest;
		this.sliceUrn = sliceUrn;
		this.sliceUuid = sliceUuid;
		this.sliceSmName = sliceSmName;
		this.sliceSmGuid = sliceSmGuid;
	}

	/**
	 * Insert the compressed version of the manifest into db. Do minimal parsing of the manifest.
	 * @param ndlMan - uncompressed
	 */
	private void insertInDb(String ndlMan) {
		Connection dbc = null;
		try {
			if (Globals.getInstance().isDbValid()) {
				Globals.info("Saving slice " + sliceUrn + " to the database " + Globals.getInstance().getDbUrl());
				// parse the manifest
				NDLManifestParser parser = new NDLManifestParser(sliceUrn, ndlMan);
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
					pst1.setString(1, man);
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
					pst1.setString(4, man);
					pst1.setString(5, MANIFEST_TYPE_GZIPPED_BASE_64_ENCODED_NDL);
					pst1.setString(6, sliceSmName);
					pst1.execute();
					pst1.close();
				}
				rs.close();
				pst.close();
			}
		} catch (SQLException e) {
			Globals.error("Unable to insert into the database: " + e);
		} catch (Exception e) {
			Globals.error("Unable to parse the manifest: " + e);
		} finally {
			if (dbc != null)
				try {
					dbc.close();
				} catch (SQLException e) {
					Globals.error("Error closing connection: " + e);
				}
		}
	}

	public void run() {
		Globals.info("Decoding/decompressing manifest for slice " + sliceUrn);
		if (Globals.getInstance().isDebugOn())
			writeToFile(man, "/tmp/rawman" + sliceUrn + "---" + sliceUuid);

		String ndlMan = null;
		try {
			ndlMan = CompressEncode.decodeDecompress(man);
		} catch (DataFormatException dfe) {
			Globals.error("Unable to decode manifest");
			return;
		}

		if (ndlMan == null)
			return;

		if (Globals.getInstance().isDebugOn())
			writeToFile(ndlMan, "/tmp/ndlman" + sliceUrn + "---" + sliceUuid);

		// insert into the database
		insertInDb(ndlMan);

		Globals.debug("Running through NDL converter");

		String rspecMan = null;
		try {
			rspecMan = callConverter(MANIFEST_TO_RSPEC, new Object[]{ndlMan, sliceUrn});
		} catch (MalformedURLException e) {
			Globals.error("NDL Converter URL error: " + Globals.getInstance().getConverters());
			return;
		} catch (Exception e) {
			Globals.error("Error converting NDL manifest: " + e.getMessage());
			return;
		}

		if (Globals.getInstance().isDebugOn())
			writeToFile(rspecMan, "/tmp/rspecman" + sliceUrn + "---" + sliceUuid);

		// publish
		URI pUrl;
		try {
			pUrl = new URI(Globals.getInstance().getPublishUrl());
		} catch (URISyntaxException e) {
			Globals.error("Error publishing to invalid URL: " + Globals.getInstance().getPublishUrl());
			return;
		}

		// exec or file or http(s)
		if ("exec".equals(pUrl.getScheme())) {
			// run through an executable
			File tmpF = null;
			try {
				Globals.info("Running through script " + pUrl.getPath());
				tmpF = File.createTempFile("manifest", null);
				String tmpFName = tmpF.getCanonicalPath();
				writeToFile(rspecMan, tmpF);
				ArrayList<String> myCommand = new ArrayList<String>();

				myCommand.add(pUrl.getPath());
				myCommand.add(tmpFName);

				String resp = executeCommand(myCommand, null);

				Globals.info("Output from script " + pUrl.getPath() + ": " + resp);
			} catch (IOException ie) {
				Globals.error("Unable to save to temp file");
			} finally {
				if (tmpF != null)
					tmpF.delete();
			}
		} else 	if ("file".equals(pUrl.getScheme())) {
			// save to file
			writeToFile(rspecMan, pUrl.getPath() + "-" + sliceUrn + "---" + sliceUuid);
		} else if ("http".equals(pUrl.getScheme()) || "https".equals(pUrl.getScheme())) {
			// push
			try {
				URL u = new URL(Globals.getInstance().getPublishUrl());
				HttpURLConnection httpCon = (HttpURLConnection) u.openConnection();
				httpCon.setDoOutput(true);
				httpCon.setRequestMethod("PUT");
				OutputStreamWriter out = new OutputStreamWriter(httpCon.getOutputStream());
				out.write(rspecMan);
				out.close();
			} catch (IOException ioe) {
				Globals.error("Unable to open connection to " + pUrl);
			}
		}
	}

	private void writeToFile(String man, String name) {
		Globals.info("Writing manifest to file " + name);

		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(name));
			bw.write(man);
			bw.close();
		} catch (IOException e) {
			Globals.error("Unable to write manifest to file " + name);
		}
	}

	private void writeToFile(String man, File f) {

		try {
			String fName = f.getCanonicalPath();
			Globals.info("Writing manifest to file " + fName);
			BufferedWriter bw = new BufferedWriter(new FileWriter(f));
			bw.write(man);
			bw.close();
		} catch (IOException e) {
			Globals.error("Unable to write manifest to file");
		}
	}
	/**
	 * Make RR calls to converters until success or list exhausted
	 * @param call
	 * @param params
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected String callConverter(String call, Object[] params) throws MalformedURLException, Exception  {
		Map<String, Object> ret = null;

		// make a round robin call to all converters (list is shuffled to do load balancing)
		Globals.debug("Choosing NDL converter from list: " + Globals.getInstance().getConverters());
		ArrayList<String> urlList = new ArrayList<String>(Arrays.asList(Globals.getInstance().getConverters().split(",")));
		Collections.shuffle(urlList);
		for(String cUrl: urlList) {
			try {
				XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
				config.setServerURL(new URL(cUrl.trim()));
				XmlRpcClient client = new XmlRpcClient();
				client.setConfig(config);

				Globals.debug("Invoking NDL converter " + call + " at " + cUrl);
				ret = (Map<String, Object>)client.execute(call, params);

				break;
			} catch (XmlRpcException e) {
				// skip it
				Globals.error("Unable to contact NDL converter at " + cUrl + " due to " + e);
				continue;
			} catch (ClassCastException ce) {
				// old converter, skip it
				Globals.error("Unable to use NDL converter at " + cUrl + " because converter return does not match expected type");
				continue;
			}
		}

		if (ret == null) {
			throw new Exception("Unable to contact/get response any converters from " + Globals.getInstance().getConverters());
		}

		if ((ret.get("err") != null) && ((Boolean)ret.get("err") == true))
			throw new Exception("Converter returned an error " + ret.get("msg"));

		if (ret.get("ret") == null)
			throw new Exception ("Converter returned null");

		return (String)ret.get("ret");
	}

	private String executeCommand(List<String> cmd, Properties env) {
		SystemExecutor se = new SystemExecutor();

		String response = null;
		try {
			response = se.execute(cmd, env, null, (Reader)null);
		} catch (RuntimeException re) {
			Globals.error("Unable to execute command " + cmd + ": " + re);
		}
		return response;
	}
}
