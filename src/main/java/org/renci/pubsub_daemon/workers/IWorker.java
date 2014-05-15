package org.renci.pubsub_daemon.workers;

/**
 * Worker interface for manifest processors. Multiple workers can be invoked on 
 * the same manifest. Workers must be registered with ManifestWorkerThread
 * @author ibaldin
 *
 */
public interface IWorker {
	
	/**
	 * Provide the name/description of this worker plugin
	 * @return
	 */
	public String getName();
}
