package org.renci.pubsub_daemon.workers;

import java.util.List;
import java.util.Map;

import org.renci.pubsub_daemon.Globals;

/**
 * Worker interface for manifest processors. Multiple workers can be invoked on 
 * the same manifest. Workers must be registered with ManifestWorkerThread
 * @author ibaldin
 *
 */
public abstract class AbstractWorker {
	
	public enum DocType { NDL_MANIFEST, COMPRESSED_NDL_MANIFEST, RSPEC_MANIFEST };
	protected Map<DocType, String> manifests;
	
	/**
	 * Provide the name/description of this worker plugin
	 * @return
	 */
	public abstract String getName();
	
	/**
	 * Tell us which types of manifests/other documents you may need
	 * @return
	 */
	public abstract List<DocType> listDocTypes();

	/**
	 * Process manifests of different types (as indicated by listManifestTypes())
	 * @param manifests
	 * @param sliceUrn
	 * @param sliceUuid
	 * @param sliceSmName
	 * @param sliceSmGuid
	 * @throws RuntimeException
	 */
	public abstract void processManifest(Map<DocType, String> manifests, String sliceUrn, String sliceUuid, String sliceSmName, String sliceSmGuid) throws RuntimeException;
	
	/**
	 * Check that the supplied manifests are of the right types
	 * @param manifests
	 */
	protected void checkManifests(Map<DocType, String> manifests) {
		for (DocType t: listDocTypes()) {
			if (manifests.get(t) == null)
				throw new RuntimeException("Expected manifest type " + t + " not available");
		}
	}

	/**
	 * Get a configuration property from properties file (null if not set)
	 * @param name
	 * @return
	 */
	public static String getConfigProperty(String name) {
		return Globals.getInstance().getConfigProperty(name);
	}
}
