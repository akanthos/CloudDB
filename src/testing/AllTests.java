package testing;

import java.io.IOException;

import app_kvEcs.ECSClient;
import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;

import static java.lang.Thread.sleep;


public class AllTests {

	private static ECSClient Ecs;

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			new Thread(new Runnable() {
				public void run() { new KVServer("127.0.0.1", 50000, 20, "FIFO", "Test");}
			}).start();


//			new Thread(new Runnable() {
//				public void run() {
//					Ecs = new ECSClient("ecs.config.small");
//					Ecs.ECSinit("3", "10", "FIFO");
//				}
//			}).start();

//			try {
//				Thread.sleep(3000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		try {
			sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
//		clientSuite.addTestSuite(ConnectionTest.class);
//		clientSuite.addTestSuite(InteractionTest.class);
//		clientSuite.addTestSuite(AdditionalTest.class);
//		clientSuite.addTestSuite(KVCacheTest.class);
		clientSuite.addTestSuite(KVServiceBasicTest.class);
		return clientSuite;
	}
	
}
