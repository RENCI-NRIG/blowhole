package org.renci.pubsub_daemon;

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
import org.renci.pubsub_daemon.workers.ICompressedManifestWorker;
import org.renci.pubsub_daemon.workers.IRSpecWorker;
import org.renci.pubsub_daemon.workers.IUncompressedManifestWorker;
import org.renci.pubsub_daemon.workers.IWorker;

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

	public void run() {
		Globals.info("Decoding/decompressing manifest for slice " + sliceUrn);
		if (Globals.getInstance().isDebugOn())
			Globals.writeToFile(man, "/tmp/rawman" + sliceUrn + "---" + sliceUuid);

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
			Globals.writeToFile(ndlMan, "/tmp/ndlman" + sliceUrn + "---" + sliceUuid);
		
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
			Globals.writeToFile(rspecMan, "/tmp/rspecman" + sliceUrn + "---" + sliceUuid);
		
		// go through the workers and let them process
		for(IWorker w: Globals.getInstance().getWorkers()) {
			Globals.info("Processing manifest with " + w.getName());
			try {
				if (w instanceof ICompressedManifestWorker) {
					ICompressedManifestWorker icmw = (ICompressedManifestWorker)w;
					icmw.processManifest(ndlMan, man, sliceUrn, sliceUuid, sliceSmName, sliceSmGuid);
				} else if (w instanceof IUncompressedManifestWorker) {	
					IUncompressedManifestWorker iumw = (IUncompressedManifestWorker)w;
					iumw.processManifest(ndlMan, sliceUrn, sliceUuid, sliceSmName, sliceSmGuid);
				} else if (w instanceof IRSpecWorker) {
					IRSpecWorker irw = (IRSpecWorker)w;
					irw.processManifest(ndlMan, rspecMan, sliceUrn, sliceUuid, sliceSmName, sliceSmGuid);
				} else 
					Globals.error("Unknown worker type: " + w.getClass().getCanonicalName());
			} catch(RuntimeException re) {
				Globals.error("Unable to process due to: " + re);
			}
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
