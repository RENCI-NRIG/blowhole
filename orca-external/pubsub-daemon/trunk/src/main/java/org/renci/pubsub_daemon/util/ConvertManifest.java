package org.renci.pubsub_daemon.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;
import org.renci.pubsub_daemon.Globals;
import org.renci.pubsub_daemon.ManifestSubscriber;

public class ConvertManifest {
	private static final String MANIFEST_TO_RSPEC = "ndlConverter.manifestToRSpec3";
	protected Properties prefProperties = null;
	
	public ConvertManifest(String fName, String newFile) {
		if (fName == null) {
			System.err.println("Unvalid file name parameters");
			return;
		}
		
		processPreferences();
		String converters = prefProperties.getProperty(ManifestSubscriber.PUBSUB_CONVERTER_LIST);
		if (converters == null) {
			System.err.println("You must specify " + ManifestSubscriber.PUBSUB_CONVERTER_LIST + " - a comma-separated list of NDL converter URLs");
			System.exit(1);
		}
		
		Globals.getInstance().setConverters(converters);
		String man = null;
		try {
			man = readFile(fName);
		} catch (IOException e) {
			System.err.println("Error reading manifest file " + fName);
		}
		
		String rspecMan = null;
		try {
			rspecMan = callConverter(MANIFEST_TO_RSPEC, new Object[]{man, "urn:urn"});
		} catch (MalformedURLException e) {
			System.err.println("NDL Converter URL error: " + Globals.getInstance().getConverters());
			return;
		} catch (Exception e) {
			System.err.println("Error converting NDL manifest: " + e);
			return;
		}
		
		// write to new file
		if (newFile.equals("-")) {
			System.out.println(rspecMan);
		} else {
			try {
				PrintWriter pw = new PrintWriter(new File(newFile));
				pw.write(rspecMan);
				pw.close();
			} catch (Exception e) {
				System.err.println("Unable to save converted manifest to " + newFile);
			}
		}
	}
	
	private static String readFile(String path) throws IOException {
		FileInputStream stream = new FileInputStream(new File(path));
		try {
			FileChannel fc = stream.getChannel();
			MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
			/* Instead of using default, pass in a decoder. */
			return Charset.defaultCharset().decode(bb).toString();
		} 
		finally {
			stream.close();
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
		ArrayList<String> urlList = new ArrayList<String>(Arrays.asList(Globals.getInstance().getConverters().split(",")));
		Collections.shuffle(urlList);
		for(String cUrl: urlList) {
			try {
				XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
				config.setServerURL(new URL(cUrl.trim()));
				XmlRpcClient client = new XmlRpcClient();
				client.setConfig(config);

				ret = (Map<String, Object>)client.execute(call, params);

				break;
			} catch (XmlRpcException e) {
				// skip it
				System.err.println("Unable to contact NDL converter at " + cUrl + " due to " + e);
				continue;
			} catch (ClassCastException ce) {
				// old converter, skip it
				System.err.println("Unable to use NDL converter at " + cUrl + " because converter return does not match expected type");
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
	
	/**
	 * Read and process preferences file
	 */
	protected void processPreferences() {
		Properties p = System.getProperties();

		// properties can be under /etc/blowhole/xmpp.properties or under $HOME/.xmpp.properties
		// in that order of preference
		String prefFilePath = ManifestSubscriber.GLOBAL_PREF_FILE;
		
		try {
			prefProperties = loadProperties(prefFilePath);
			return;
		} catch (IOException ioe) {
			;
		}
		
		prefFilePath = "" + p.getProperty("user.home") + p.getProperty("file.separator") + ManifestSubscriber.PREF_FILE;
		try {
			prefProperties = loadProperties(prefFilePath);
		} catch (IOException e) {
			System.err.println("Unable to load local config file " + prefFilePath + ", exiting.");
			InputStream is = Class.class.getResourceAsStream("/org/renci/pubsub_daemon/xmpp.sample.properties");
			if (is != null) {
				try {
					String s = new java.util.Scanner(is).useDelimiter("\\A").next();
					System.err.println("Create $HOME/.xmpp.properties file as follows: \n\n" + s);
				} catch (java.util.NoSuchElementException ee) {
					;
				}
			} else {
				System.err.println("Unable to load sample properties");
			}
			System.exit(1);
		}
	}
	
	private Properties loadProperties(String fileName) throws IOException {
		File prefs = new File(fileName);
		FileInputStream is = new FileInputStream(prefs);
		BufferedReader bin = new BufferedReader(new InputStreamReader(is, "UTF-8"));

		Properties p = new Properties();
		p.load(bin);
		bin.close();
		
		return p;
	}
	
	public static void main(String[] argv) {
		if (argv.length != 2) {
			System.err.println("Usage: convert  <ndl manifest> -|<rspec manifest>");
			System.exit(1);
		}

		ConvertManifest cm = new ConvertManifest(argv[0], argv[1]);
	}
}
