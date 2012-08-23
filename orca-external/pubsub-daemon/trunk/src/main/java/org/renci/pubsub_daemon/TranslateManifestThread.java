package org.renci.pubsub_daemon;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.zip.DataFormatException;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;

/**
 * This thread takes a base-64-encoded and gzipped manifest,
 * uncompresses it, decodes via remote XMLRPC-NDL converter
 * function and publishes it to a desired URL.
 * 
 * @author ibaldin
 *
 */
public class TranslateManifestThread implements Runnable {
	private static final String MANIFEST_TO_RSPEC = "ndlConverter.manifestToRSpec3";
	private final String man;
	private final String sliceUrn;

	TranslateManifestThread(String manifest, String sliceUrn) {
		man = manifest;
		this.sliceUrn = sliceUrn;
	}

	public void run() {
		Globals.info("Decoding/decompressing manifest for slice " + sliceUrn);
		if (Globals.getInstance().isDebugOn())
			writeToFile(man, "/tmp/rawman"+sliceUrn);
		
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
			writeToFile(ndlMan, "/tmp/ndlman"+sliceUrn);
		
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
			writeToFile(rspecMan, "/tmp/rspecman"+sliceUrn);
		
		// publish
		URL pUrl;
		try {
			pUrl = new URL(Globals.getInstance().getPublishUrl());
		} catch (MalformedURLException e) {
			Globals.error("Error publishing to invalid URL: " + Globals.getInstance().getPublishUrl());
			return;
		}
		
		// file or http(s)
		if ("file".equals(pUrl.getProtocol())) {
			// save to file
			writeToFile(rspecMan, pUrl.getPath() + "-" + sliceUrn);
		} else {
			// push
			try {
				HttpURLConnection httpCon = (HttpURLConnection) pUrl.openConnection();
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

}
