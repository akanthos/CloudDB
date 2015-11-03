package testing;

import app_kvServer.KVCache;
import client.KVStore;
import common.messages.KVMessage;
import org.junit.Test;

import junit.framework.TestCase;

import java.util.Random;
import java.util.concurrent.*;

public class AdditionalTest extends TestCase {

	private ExecutorService threadpool = null;

	public void setUp() {
		threadpool = Executors.newFixedThreadPool(5);
	}

	public void tearDown() {
		threadpool.shutdown();
	}


	@Test
	public void testConnectManyClients() {
		Exception[] ex = new Exception[5];

		for(int i = 0; i < 5; i++) {
			KVStore kvClient = new KVStore("localhost", 50000);
			try {
				kvClient.connect();
			} catch (Exception e) {
				ex[i] = e;
			}
		}

		assertNull(ex[0]);
		assertNull(ex[1]);
		assertNull(ex[2]);
		assertNull(ex[3]);
		assertNull(ex[4]);
	}


	/**
	 * This test creates 5 clients and they all send a put command
	 * to the server. The results returned by the server need to be
	 * all PUT_SUCCESS.
	 */
	@Test
	public void testPutResultManyClients() {
		Future<KVMessage>[] futures = new Future[5];
		try {
			for(int i = 0; i < 5; i++)
			{
				futures[i] = threadpool.submit(new ClientResult(i));
			}
			assertTrue(futures[0].get().getStatus() == KVMessage.StatusType.PUT_SUCCESS);
			assertTrue(futures[1].get().getStatus() == KVMessage.StatusType.PUT_SUCCESS);
			assertTrue(futures[2].get().getStatus() == KVMessage.StatusType.PUT_SUCCESS);
			assertTrue(futures[3].get().getStatus() == KVMessage.StatusType.PUT_SUCCESS);
			assertTrue(futures[4].get().getStatus() == KVMessage.StatusType.PUT_SUCCESS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	class Client implements Callable<Exception> {
		protected KVStore kvClient;
		protected Integer i;

		public Client(Integer i) throws Exception{
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
		public Exception call() {
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
			return ex;
		}
	}

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

