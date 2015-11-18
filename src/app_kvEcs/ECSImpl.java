package app_kvEcs;

import com.sun.corba.se.spi.activation.Server;
import common.ServerInfo;
import common.messages.KVAdminMessage;
import common.messages.KVAdminMessageImpl;
import common.messages.KVMessage;
import hashing.MD5Hash;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class ECSImpl implements ECS {

    private List<ServerInfo> allServers;
    private List<ServerInfo> activeServers;
    private ConfigReader confReader;
    private MD5Hash md5Hasher;
    private static Logger logger = Logger.getLogger(ECSImpl.class);
    private int cacheSize;
    private String displacementStrategy;
    private boolean running;
    private boolean local=true;
    private SshCommunication processInv;
    //map handling storing <ServerInfo>--<SocketConnection>
    private Map<ServerInfo, KVConnection> KVConnections;

    /**
     *
     */
    public ECSImpl(String fileName) throws IOException {
        try {
            this.confReader = new ConfigReader( fileName );
            allServers = confReader.getServers();
        } catch (IOException e) {
            throw new IOException("ECSImpl. Cannot access ecs.config");
        }
    }

    /**
     * Randomly choose <numberOfNodes> servers from the available machines and start the KVServer
     * by issuing a SSH call to the respective machine.
     * This call launches the server with the specified cache size and displacement strategy.
     * You can assume that the KVServer.jar is located in the same directory as the ECSImpl.
     * All servers are initialized with the meta-data and remain in state stopped
     * @param numberOfNodes
     * @param cacheSize
     * @param displacementStrategy
     * @return true if succeeded else false
     */
    public boolean initService(int numberOfNodes, int cacheSize, String displacementStrategy) {

        running = true;
        Random rand = new Random();
        int count = 0;
        processInv = new SshCaller();

        ServerInfo tmp;
        List<ServerInfo> startServers = new ArrayList<ServerInfo>();
        if (this.activeServers == null)
            this.activeServers = new ArrayList<ServerInfo>();
        // Choose random servers to start
        while (count < numberOfNodes) {
            int i = rand.nextInt(allServers.size());
            tmp = allServers.get(i);
            //neither active nor already randomly selected
            if ((!startServers.contains(tmp)) && !this.activeServers.contains(tmp)) {
                startServers.add(tmp);
                count++;
            }
        }
        logger.info("ECS launching " + numberOfNodes + " servers.");

        //start the store servers
        startServers = launchNodes(startServers, cacheSize, displacementStrategy);
        //calculate the meta-data => List <ServerInfo>
        startServers = calculateMetaData(startServers);
        // communicate with servers and send call initialize command
        KVAdminMessageImpl initMsg = (KVAdminMessageImpl) InitMsg(startServers);

        // create server connection for further communication with the servers
        for (ServerInfo server : this.activeServers) {
            KVConnection kvconnection = new KVConnection(server);
            try {
                kvconnection.connect();
                kvconnection.sendMessage(initMsg);
                KVConnections.put(server, kvconnection);
                kvconnection.disconnect();
            } catch (IOException e) {
                logger.error("One server node couldn't be initiated" + server);
            }
        }



        return true;
    }

    /**
     * Create an INIT KVAdminMessage
     * @param startServers
     * @return
     */
    private KVAdminMessage InitMsg(List<ServerInfo> startServers) {
        KVAdminMessageImpl initMsg = new KVAdminMessageImpl();
        initMsg.setStatus(KVAdminMessage.StatusType.INIT);
        initMsg.setMetadata(startServers);
        return initMsg;
    }


    /**
     *
     * @param startServers
     * @param cacheSize
     * @param displacementStrategy
     * @return
     */
    private List<ServerInfo> launchNodes(List<ServerInfo> startServers, int cacheSize, String displacementStrategy) {
		/*
		 * it is considered that the invoker and invoked processes are in the
		 * same folder and machine
		 */
        String path = System.getProperty("user.dir");
        String command = "nohup java -jar " + path + "/ms3-server.jar ";
        String arguments[] = new String[4];
        arguments[1] = String.valueOf(cacheSize);
        arguments[2] = displacementStrategy;
        arguments[3] = " ERROR &";
        int result;

        Iterator<ServerInfo> iterator = startServers.iterator();
        while (iterator.hasNext()) {
            ServerInfo item = iterator.next();
            arguments[0] = String.valueOf(item.getServerPort());

            // for ssh calls
            if (!local)
                result = processInv.invokeProcessRemotely(item.getAddress(),
                        command, arguments);

                // for local calls (for local testing, calls are made without SSH)
            else
                result = processInv.invokeProcessLocally(command, arguments);

            // the server started successfully
            if (result == 0) {
                this.activeServers.add(item);
                item.setLaunched(true);

            }
            // could not start the server
            else
                iterator.remove();
        }
        return startServers;
    }

    /**x
     * Starts the storage service; By calling start() on all
     * KVServer instances that participate the service
     * @return true if succeeded else false
     */
    public boolean start(){
        return true;
    }

    /**
     * Stops the service; all participating KVServers are stopped for
     * processing client requests but the processes remain running.
     * @return true if succeeded else false
     */
    public boolean stop(){
        return true;
    }

    /**
     * Stops all server instances and exits the remote processes.
     * @return true if succeeded else false
     */
    public boolean shutdown() { return true; }



    /**
     * Create a new KVServer with the specified cache size and displacement strategy
     * and add it to the storage service at an arbitrary position.
     * @param cacheSize
     * @param displacementStrategy
     * @return true if succeeded else false
     */
    @Override
    public boolean addNode(int cacheSize, String displacementStrategy){
        return true;
    }

    /**
     * Remove a node from the storage service at an arbitrary position.
     * @return true if succeeded else false
     */
    public boolean removeNode(){
        return true;
    }

    /**
     * Lock the KVServer for write operations.
     * @return true if succeeded else false
     */
    public boolean lockWrite( ServerInfo server){
        return true;
    }

    /**
     * Unlock the KVServer for write operations.
     * @return true if succeeded else false
     */
    public boolean unlockWrite( ServerInfo server){
        return true;
    }

    /**
     * Start a Store Server using ssh
     * @param node
     * @return
     */
    public boolean startNode(ServerInfo node){
        return true;
    }
    /**
     * Calculate metaData of the current ECS system
     *
     * @param servers servers of the current system
     * @return the new metaData
     */
    private List<ServerInfo> calculateMetaData(List<ServerInfo> servers){

        // calculate each server's MD5 Hash value and sort them based on this
        // value

        for (ServerInfo server : servers) {
            long hashKey = md5Hasher.hash(server.getAddress() + ":"
                    + server.getServerPort());
            server.setToIndex(hashKey);
        }
        Collections.sort(servers, new Comparator<ServerInfo>() {
            @Override
            public int compare(ServerInfo o1, ServerInfo o2) {
                if (o1.getToIndex()>o2.getToIndex())
                    return 1;
                else
                    return 0;
            }
        });

        // setting predecessor
        for (int i = 0; i < servers.size(); i++) {
            ServerInfo server = servers.get(i);
            ServerInfo predecessor;
            if (i == 0) {
                // first node
                predecessor = servers.get(servers.size() - 1);
            } else {
                predecessor = servers.get(i - 1);
            }
            server.setFromIndex(predecessor.getToIndex());
        }
        return servers;
    }
}
