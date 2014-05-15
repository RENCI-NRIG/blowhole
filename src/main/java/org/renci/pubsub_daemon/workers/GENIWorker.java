package org.renci.pubsub_daemon.workers;

public class GENIWorker implements IRSpecWorker {
	public static final String GENIWorkerName = "GENI Manifest worker puts RSpec elements into the database";

	@Override
	public String getName() {
		return GENIWorkerName;
	}

	@Override
	public void processManifest(String manifest, String rSpec, String sliceUrn,
			String sliceUuid, String sliceSmName, String sliceSmGuid)
			throws RuntimeException {
		
	}

}
