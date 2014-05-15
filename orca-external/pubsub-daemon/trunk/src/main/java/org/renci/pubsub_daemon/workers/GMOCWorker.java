package org.renci.pubsub_daemon.workers;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;

import org.renci.pubsub_daemon.Globals;

public class GMOCWorker implements IRSpecWorker {
	public static final String GMOCWorkerName = "GMOC Manifest worker puts submits manifest to GMOC";

	@Override
	public String getName() {
		return GMOCWorkerName;
	}

	@Override
	public void processManifest(String manifest, String rspecMan, String sliceUrn,
			String sliceUuid, String sliceSmName, String sliceSmGuid)
			throws RuntimeException {
		
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
				Globals.writeToFile(rspecMan, tmpF);
				ArrayList<String> myCommand = new ArrayList<String>();

				myCommand.add(pUrl.getPath());
				myCommand.add(tmpFName);

				String resp = Globals.executeCommand(myCommand, null);

				Globals.info("Output from script " + pUrl.getPath() + ": " + resp);
			} catch (IOException ie) {
				Globals.error("Unable to save to temp file");
			} finally {
				if (tmpF != null)
					tmpF.delete();
			}
		} else 	if ("file".equals(pUrl.getScheme())) {
			// save to file
			Globals.writeToFile(rspecMan, pUrl.getPath() + "-" + sliceUrn + "---" + sliceUuid);
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

}
