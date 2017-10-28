package com.itemsense.client.simple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thingworx.communications.client.ClientConfigurator;

public class ThingworxClientFactory {

	private static final Logger LOG = LoggerFactory.getLogger(ThingworxClientFactory.class);

	public static ThingworxClient getClient() {
		
		ClientConfigurator config = new ClientConfigurator();
		ThingworxClient client = null;

		// Set the URI of the server that we are going to connect to.
		config.setUri(ThingworxConfig.THINGWORX_WS_URL);

		// Set the Application Key. This will allow the client to authenticate
		config.setAppKey(ThingworxConfig.APP_KEY);

		// This will allow us to test against a server using a self-signed
		// certificate.
		config.ignoreSSLErrors(true); // All self signed certs

		try {
			client = new ThingworxClient(config);
			client.start();
			while (!client.getEndpoint().isConnected()) {
				Thread.sleep(5000);
			} 
		} catch (Exception e) {
			LOG.error("An exception occured during execution.", e);
		}
				
		return client;
	}

	
}
