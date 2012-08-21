package org.renci.pubsub_daemon;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.zip.DataFormatException;

import org.apache.log4j.Logger;
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
	private static final String MANIFEST_TO_RSPEC = "";
	private final String man;
	private final String sliceUrn;
	private final Logger logger;
	private final String converters;
	private final Boolean compression;

	TranslateManifestThread(String manifest, String sliceUrn, String converters, Boolean compression, Logger l) {
		man = manifest;
		logger = l;
		this.sliceUrn = sliceUrn;
		this.converters = converters;
		this.compression = compression;
	}

	public void run() {
		logger.info("Decoding/decompressing manifest for slice " + sliceUrn);
		String ndlMan = null;
		try {
			ndlMan = CompressEncode.decodeDecompress(man);
		} catch (DataFormatException dfe) {
			logger.error("Unable to decode manifest");
			writeToFile(man, "/tmp/rawman");
			return;
		}
		
		if (ndlMan == null)
			return;

		logger.debug("Running through NDL converter");
		
		String rspecMan = null;
		try {
			rspecMan = callConverter(MANIFEST_TO_RSPEC, new Object[]{ndlMan, sliceUrn});
		} catch (MalformedURLException e) {
			logger.error("NDL Converter URL error: " + converters);
			return;
		} catch (Exception e) {
			logger.error("Error converting NDL manifest: " + e.getMessage());
			return;
		}
		
		writeToFile(rspecMan, "/tmp/rspecman");
	}

	private void writeToFile(String man, String name) {
		logger.info("Writing manifest to file " + name);

		// write to file for now
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(name));
			bw.write(man);
		} catch (IOException e) {
			logger.error("Unable to write manifest to file " + name);
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
		logger.debug("Choosing NDL converter from list: " + converters);
		ArrayList<String> urlList = new ArrayList<String>(Arrays.asList(converters.split(",")));
		Collections.shuffle(urlList);
		for(String cUrl: urlList) {
			try {
				XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
				config.setServerURL(new URL(cUrl.trim()));
				XmlRpcClient client = new XmlRpcClient();
				client.setConfig(config);

				logger.debug("Invoking NDL converter " + call + " at " + cUrl);
				ret = (Map<String, Object>)client.execute(call, params);

				break;
			} catch (XmlRpcException e) {
				// skip it
				logger.error("Unable to contact NDL converter at " + cUrl + " due to " + e);
				continue;
			} catch (ClassCastException ce) {
				// old converter, skip it
				logger.error("Unable to use NDL converter at " + cUrl + " because converter return does not match expected type");
				continue;
			}
		}

		if (ret == null) {
			throw new Exception("Unable to contact/get response any converters from " + converters);
		}

		if ((ret.get("err") != null) && ((Boolean)ret.get("err") == true))
			throw new Exception("Converter returned an error " + ret.get("msg"));

		if (ret.get("ret") == null)
			throw new Exception ("Converter returned null");

		return (String)ret.get("ret");
	}

}
