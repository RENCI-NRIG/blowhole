package org.renci.pubsub_daemon.workers;

public interface IRSpecWorker extends IWorker {
	/**
	 * Process RSpec manifest
	 * @param manifest - NDL
	 * @param rSpec manifest - RSpec
	 * @param sliceUrn
	 * @param sliceUuid
	 * @param sliceSmName
	 * @param sliceSmGuid
	 * @throws RuntimeException
	 */
	public void processManifest(String manifest, String rSpec, String sliceUrn, String sliceUuid, String sliceSmName, String sliceSmGuid) throws RuntimeException;
}
