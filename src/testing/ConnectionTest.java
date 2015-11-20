package testing;

import java.net.UnknownHostException;

import client.KVStore;

import junit.framework.TestCase;


public class ConnectionTest extends TestCase {

	
	public void testConnectionSuccess() {

		Exception ex = null;

		KVStore kvClient = new KVStore();
		try {
			kvClient.connect("localhost", 50000);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
	}


	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore();

		try {
			kvClient.connect("unknown", 50000);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex instanceof UnknownHostException);
	}
	
	
	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore();
		
		try {
			kvClient.connect("localhost", 123456789);
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}
	
	

	
}

