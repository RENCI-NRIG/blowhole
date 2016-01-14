package org.renci.pubsub_daemon.workers;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.renci.pubsub_daemon.Globals;

public class LoggingWorker extends AbstractWorker {
	private static final String LoggingWorkerName = "A simple worker that logs events without doing anything";
	
	@Override
	public String getName() {
		return LoggingWorkerName;
	}

	@Override
	public List<DocType> listDocTypes() {
		return Arrays.asList(AbstractWorker.DocType.COMPRESSED_NDL_MANIFEST, AbstractWorker.DocType.NDL_MANIFEST);
	}

	@Override
	public void runAtStartup() {

	}

	@Override
	public void processManifest(Map<DocType, String> manifests,
			String sliceUrn, String sliceUuid, String sliceSmName,
			String sliceSmGuid) throws RuntimeException {
		Globals.info("Logging slice " + sliceUrn + " with uuid " + sliceUuid + " from " + sliceSmName + " with SM UUID " + sliceSmGuid);
	}

}
