package testing;

import app_kvEcs.ECSCore;
import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import static org.junit.Assert.assertTrue;

public class ms3_tests {


    private static KVStore client;
    private static ECSCore ecs;

    @Before
    public  void setUp(){
        try {
            ecs = new ECSCore( "ecs.config" );
            ecs.initService( 10, 8, "FIFO");
            client = new KVStore (  );
            client.connect ("localhost", 50000);
        } catch ( IOException e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testServerStopped(){
        Exception ex = null;
        KVMessageImpl response= null;
        try {
            ecs.stop();
            Thread.sleep ( 3000 );
            response = (KVMessageImpl)client.put ( "key1" , "value1" );
        } catch ( IOException e ) {
            e.printStackTrace();
        } catch ( InterruptedException e ) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(response.getStatus ().equals ( KVMessage.StatusType.SERVER_STOPPED ));
    }


    @Test
    public void testServerWriteLock(){
        Exception ex = null;
        KVMessageImpl response= null;
        KVMessageImpl clientMessage  = null;
        try {
            ecs.start();
            ecs.lockWrite();
            Thread.sleep ( 3000 );
            response = (KVMessageImpl)client.put ( "key1" , "value1" );
            clientMessage = (KVMessageImpl)response;
        } catch ( IOException e ) {
            e.printStackTrace();
        } catch ( InterruptedException e ) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(response.getStatus ().equals ( KVMessage.StatusType.SERVER_WRITE_LOCK ));
        ecs.unlockWrite();
    }

    @Test
    public void testServerPutSuccess(){
        Exception ex = null;
        KVMessageImpl response= null;
        try {
            ecs.start();
            Thread.sleep ( 3000 );
            response = (KVMessageImpl)client.put ( "psomi" , "tiri" );
        } catch ( IOException e ) {
            e.printStackTrace();
        } catch ( InterruptedException e ) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(response.getStatus());
        assertTrue((response.getStatus().equals( KVMessage.StatusType.PUT_SUCCESS)));
    }

    @After
    public void tearDown(){
        client.disconnect(true);
        ecs.shutdown();
        ecs = null;
        client = null;
    }

}

