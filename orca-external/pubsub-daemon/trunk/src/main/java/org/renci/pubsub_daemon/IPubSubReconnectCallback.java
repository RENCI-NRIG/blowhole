package org.renci.pubsub_daemon;

public interface IPubSubReconnectCallback {

	public void onReconnect();
	public String name();
}
