package org.renci.pubsub_daemon.workers;

public interface ICompressedManifestWorker extends IWorker {

	/**
	 * Manifest processor 
	 * @param manifest - uncompressed manifest
	 * @param compressedManifest - compressed/gzipped manifest string
	 * @param sliceUrn - slice URN
	 * @param sliceUuid - slice GUID
	 * @param sliceSmName - name of the SM for the slice
	 * @param sliceSmGuid - GUID of the SM for the slice
	 * @throws RuntimeException
	 */
	public void processManifest(String manifest, String compressedManifest, String sliceUrn, String sliceUuid, String sliceSmName, String sliceSmGuid) throws RuntimeException;

}
