package testing;

import app_kvServer.KVCache;
import client.KVStore;
import common.messages.KVMessage;
import org.junit.Test;

import junit.framework.TestCase;

import java.util.Random;
import java.util.concurrent.*;

public class AdditionalTest extends TestCase {

	private ExecutorService threadpool = Executors.newFixedThreadPool(5);

	public void setUp() {


	}

	public void tearDown() {

	}


	@Test
	public void testConnectManyClients() {
		final Integer NUMBER_OF_CLIENTS = 500;
		Exception[] ex = new Exception[NUMBER_OF_CLIENTS];

		for(int i = 0; i < NUMBER_OF_CLIENTS; i++) {
			KVStore client = new KVStore("localhost", 50000);
			try {
				client.connect();
			} catch (Exception e) {
				ex[i] = e;
			}
		}
		for(int i = 0; i < NUMBER_OF_CLIENTS; i++) {
			assertNull(ex[i]);

		}
	}


	/**
	 * This test creates 5 clients and they all send a put command
	 * to the server. The results returned by the server need to be
	 * all PUT_SUCCESS.
	 */
//	@Test
//	public void testPutResultManyClients() {
//		Future<KVMessage>[] futures = new Future[5];
//		try {
//			for(int i = 0; i < 5; i++)
//			{
//				futures[i] = threadpool.submit(new ClientResult(i));
//			}
//			assertTrue(futures[0].get().getStatus() == KVMessage.StatusType.PUT_SUCCESS);
//			assertTrue(futures[1].get().getStatus() == KVMessage.StatusType.PUT_SUCCESS);
//			assertTrue(futures[2].get().getStatus() == KVMessage.StatusType.PUT_SUCCESS);
//			assertTrue(futures[3].get().getStatus() == KVMessage.StatusType.PUT_SUCCESS);
//			assertTrue(futures[4].get().getStatus() == KVMessage.StatusType.PUT_SUCCESS);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		} catch (ExecutionException e) {
//			e.printStackTrace();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}

	class ClientResult implements Callable<KVMessage> {
		protected KVStore kvClient;
		protected Integer i;

		public ClientResult(Integer i) throws Exception{
			this.i = i;
			kvClient = new KVStore("localhost", 50000);
			try {
				kvClient.connect();
				System.out.println("Client " + i + ": Connected");
			} catch (Exception e) {
				System.out.println("Client " + i + ": Cannot connect");
			}

		}

		@Override
		public KVMessage call() {
			String key = "foo" + i;
			String value = "bar" + i;
			KVMessage response = null;
			Exception ex = null;

			try {
				response = kvClient.put(key, value);
			} catch (Exception e) {
				ex = e;
			}
		/*response.setStatus(KVMessage.StatusType.DELETE_ERROR);
		if (ex != null) System.out.println("Client " + i + ": Exception at putting");
		if (ex == null) System.out.println("Client " + i + ": Put status: " + response.getStatus().toString());
		assertTrue(false*//*ex == null && response.getStatus() == KVMessage.StatusType.PUT_SUCCESS*//*);
		kvClient.disconnect();*/
			return response;
		}
	}
}

