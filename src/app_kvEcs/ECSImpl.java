package app_kvEcs;

import com.sun.corba.se.spi.activation.Server;
import common.Serializer;
import common.ServerInfo;
import common.messages.AbstractMessage;
import common.messages.KVAdminMessage;
import common.messages.KVAdminMessageImpl;
import common.messages.KVMessage;
import common.utils.Utilities;
import hashing.MD5Hash;
import helpers.CannotConnectException;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class ECSImpl implements ECS {

    private static Logger logger = Logger.getLogger(ECSImpl.class);
    private List<ServerInfo> allServers;
    private List<ServerInfo> activeServers;
    private ConfigReader confReader;
    private MD5Hash md5Hasher;
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
        this.md5Hasher = new MD5Hash();
        processInv = new SshCaller();
        this.KVConnections = new HashMap<ServerInfo, KVConnection>();

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
        activeServers = startServers;
        // communicate with servers and send call initialize command
        KVAdminMessageImpl initMsg = InitMsg(activeServers, cacheSize, displacementStrategy);
        // create server connection for further communication with the servers
        for (ServerInfo server : this.activeServers) {
            KVConnection connection = new KVConnection(server);
            try {
                sendECSCmd(connection, server, initMsg);
            } catch (CannotConnectException e) {
                connection.disconnect();
                logger.info(initMsg.getStatus() + " operation on server " + server.getAddress()+":"
                        +server.getServerPort() + " failed due to Connection problem");
            }
        }

        return true;
    }


    /**
     * Create an INIT KVAdminMessage
     * @param startServers
     * @return
     */
    private KVAdminMessageImpl InitMsg(List<ServerInfo> startServers, int cacheSize, String displacementStrategy) {
        KVAdminMessageImpl initMsg = new KVAdminMessageImpl();
        initMsg.setStatus(KVAdminMessage.StatusType.INIT);
        initMsg.setCacheSize(cacheSize);
        initMsg.setDisplacementStrategy(displacementStrategy);
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
        String arguments[] = new String[2];
        arguments[1] = " ERROR &";
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
    @Override
    public void start() {

        KVAdminMessageImpl startMessage = new KVAdminMessageImpl();
        startMessage.setStatus(KVAdminMessage.StatusType.START);
        KVConnection channel = null;
        byte[] byteMessage;
        for (ServerInfo server : this.activeServers) {
            try {
                sendECSCmd(KVConnections.get(server), server, startMessage);
            } catch (CannotConnectException e) {
                KVConnections.get(server).disconnect();
                logger.info(startMessage.getStatus() + " operation on server " + server.getAddress()+":"
                        +server.getServerPort() + " failed due to Connection problem");
            }
        }
        logger.info("Active servers are started.");
    }


    /**
     * Stops the service; all participating KVServers are stopped for
     * processing client requests but the processes remain running.
     * @return true if succeeded else false
     */
    @Override
    public void stop() {
        KVAdminMessageImpl stopMessage = new KVAdminMessageImpl();
        stopMessage.setStatus(KVAdminMessage.StatusType.STOP);
        KVConnection channel = null;
        byte[] byteMessage;
        for (ServerInfo server : this.activeServers) {
            try {
                channel = KVConnections.get(server);
                channel.connect();

                channel.sendMessage(stopMessage);
                byteMessage = Utilities.receive(channel.getInput());
                AbstractMessage abstractMessage = Serializer.toObject(byteMessage);
                stopMessage = (KVAdminMessageImpl) abstractMessage;
                if (stopMessage.getStatus().equals(KVAdminMessage.StatusType.OPERATION_SUCCESS))
                    System.out.println("Successfully started server" + server.getAddress() + server.getServerPort());
                else
                    System.out.println("Sat");
            } catch (IOException e) {
                channel.disconnect();
                logger.error("Could not send message to server" + server
                        + e.getMessage());
            } catch (CannotConnectException e) {
                channel.disconnect();
                e.printStackTrace();
            }
        }
        logger.info("Active servers are started.");
    }

    /**
     * Stops all server instances and exits the remote processes.
     * @return true if succeeded else false
     */
    public boolean shutdown() { return true; }


    /**
     * launch Single Server
     *
     * @param startServer
     * @return 0 in case of successful launch
     */
    private boolean launchNode(ServerInfo startServer) {

        String path = System.getProperty("user.dir");
        String command = "nohup java -jar " + path + "/ms3-server.jar ";
        String arguments[] = new String[2];
        arguments[1] = "  ERROR &";
        int result;
        arguments[0] = String.valueOf(startServer.getServerPort());

        // ssh calls
        if (!local)
            result = processInv.invokeProcessRemotely(startServer.getAddress(),
                    command, arguments);
        // for local invocations
        else
            result = processInv.invokeProcessLocally(command, arguments);

        // remote server started successfully
        if (result == 0) {
            this.activeServers.add(startServer);
            startServer.setLaunched(true);
            return true;
        } else
            return false;
    }


    /**
     * Create a new KVServer with the specified cache size and displacement strategy
     * and add it to the storage service at an arbitrary position.
     * @param cacheSize
     * @param displacementStrategy
     * @return true if succeeded else false
     */
    @Override
    public void addNode(int cacheSize, String displacementStrategy){

        logger.debug("!! SYSTEM BEFORE ADDING A NEW NODE !!");
        for (ServerInfo s : activeServers)
            logger.debug(s.getServerPort() + s.getFromIndex() + s.getToIndex() );
        int run = -1;
        ServerInfo newServer = new ServerInfo();
        Iterator<ServerInfo> allServersIterator = allServers.iterator();

        while (allServersIterator.hasNext()) {
            newServer = allServersIterator.next();
            if ( !this.activeServers.contains(newServer) ){
                //try to launch the server
                //launchNode adds server to the active Servers
                if (launchNode(newServer))
                    break;
                else {
                    logger.warn("Could not add a new Server!");
                    return;
                }
            }
        }
        if (newServer == null) {
            logger.info("No available node to add.");
            return;
        }
        //calculate the new MetaData.
        activeServers = calculateMetaData(activeServers);

        //initialize the new Server
        KVAdminMessageImpl initMsg = InitMsg(activeServers, cacheSize, displacementStrategy);
        KVConnection kvconnection = new KVConnection(newServer);
        byte[] byteMessage;
        try {
            kvconnection.connect();
            kvconnection.sendMessage(initMsg);
            byteMessage = Utilities.receive(kvconnection.getInput());
            AbstractMessage abstractMessage = Serializer.toObject(byteMessage);
            initMsg = (KVAdminMessageImpl) abstractMessage;
            if (initMsg.getStatus().equals(KVAdminMessage.StatusType.OPERATION_SUCCESS))
                KVConnections.put(newServer, kvconnection);
            else
                System.out.println("Init Operation on server " + newServer + " failed.");
            logger.info("The new node added" + newServer.getAddress() + ":"
                    + newServer.getServerPort());
        } catch (Exception e) {
            // server could not be initiated so remove it from the list!
            logger.error(" server node couldn't be initiated"
                    + newServer.getAddress() + ":" + newServer.getServerPort()
                    + " Operation addNode Not Successfull");
            activeServers.remove(newServer);
            kvconnection.disconnect();
            KVConnections.remove(kvconnection);
            calculateMetaData(activeServers);
            return;
        }
        ServerInfo successor = getSuccessor(newServer);
        //tell successor to send the
        if (sendData(successor, newServer, newServer.getFromIndex(), newServer.getToIndex()) == 0)
        {
            UpdateMetaData();
            // release the lock from the successor, new Node
            logger.debug("release the lock");
            KVAdminMessageImpl releaseLock = new KVAdminMessageImpl();
            releaseLock.setStatus(KVAdminMessage.StatusType.UNLOCK_WRITE);
            try {
                sendECSCmd(KVConnections.get(successor), successor, releaseLock);
                logger.debug("All locks are released.");
            } catch (CannotConnectException e) {
                logger.error("Release Lock message couldn't be sent.");
            }
            kvconnection.disconnect();
            return;

        }

    }

    private void sendECSCmd(KVConnection channel, ServerInfo server, KVAdminMessageImpl message) throws CannotConnectException {
        byte[] byteMessage;
        KVAdminMessageImpl result;
        try {
            channel.connect();
            channel.sendMessage(message);
            byteMessage = Utilities.receive(channel.getInput());
            AbstractMessage abstractMessage = Serializer.toObject(byteMessage);
            result = (KVAdminMessageImpl) abstractMessage;
            if (result.getStatus().equals(KVAdminMessage.StatusType.OPERATION_SUCCESS)) {
                if (message.getStatus().equals(KVAdminMessage.StatusType.INIT))
                    KVConnections.put(server, channel);
                logger.info(message.getStatus() + "Operation on server " + server.getAddress() + ":"
                        + server.getServerPort() + " succeeded.");
            }else {
                logger.info(message.getStatus() + "Operation on server " + server.getAddress() + ":"
                        + server.getServerPort() + " failed.");
                channel.disconnect();
            }
        } catch (IOException e) {
            throw new CannotConnectException(message.getStatus() + " operation on server " + server.getAddress()+":"
                    +server.getServerPort() + " failed due to Connection problem");
        }
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
     * tells fromNode to send data in range(fromIndex,toIndex) to toNode
     *
     * @param fromNode
     *            : (ServerInfo)The KVServer that sends the data
     * @param toNode
     *            : (ServerINfo) The KVServer that recieves the data
     * @param fromIndex
     * @param toIndex
     * @return 0 in case of success and -1 otherwise
     */
    private int sendData(ServerInfo fromNode, ServerInfo toNode,
                         Long fromIndex, Long toIndex) {


        KVConnection kvConnection = new KVConnection(fromNode);
        KVAdminMessageImpl lockMsg = new KVAdminMessageImpl();
        lockMsg.setStatus(KVAdminMessage.StatusType.LOCK_WRITE);
        byte[] byteMessage;
        try {
            kvConnection.connect();
            kvConnection.sendMessage(lockMsg);
            byteMessage = Utilities.receive(kvConnection.getInput());
            AbstractMessage abstractMessage = Serializer.toObject(byteMessage);
            lockMsg = (KVAdminMessageImpl) abstractMessage;
            if (lockMsg.getStatus().equals(KVAdminMessage.StatusType.OPERATION_SUCCESS))
                logger.info("Write lock operation on: " + fromNode.getAddress() +":"+
                fromNode.getServerPort()+ " succeeded.");
            else {
                logger.info("Write lock operation on: " + fromNode.getAddress() + ":" +
                        fromNode.getServerPort() + " failed.");
                return -1;
            }
        } catch (Exception e) {
            logger.error("WriteLock message couldn't be sent to "
                    + fromNode.getServerPort());
            kvConnection.disconnect();
            return -1;
        }

        // send move data message to Successor(fromNode)
        KVAdminMessageImpl moveDataMsg = new KVAdminMessageImpl();
        moveDataMsg.setStatus(KVAdminMessage.StatusType.MOVE_DATA);
        moveDataMsg.setLow(fromIndex);
        moveDataMsg.setHigh(toIndex);
        moveDataMsg.setServerInfo(toNode);
        byte[] movebyteResponse;
        try {
            kvConnection.sendMessage(moveDataMsg);
            movebyteResponse = Utilities.receive(kvConnection.getInput());
            AbstractMessage abstractMessage = Serializer.toObject(movebyteResponse);
            moveDataMsg = (KVAdminMessageImpl) abstractMessage;
            if (moveDataMsg.getStatus().equals(KVAdminMessage.StatusType.OPERATION_SUCCESS))
                logger.info("Mode data on: " + fromNode.getAddress() +":"+
                        fromNode.getServerPort()+ " succeeded.");
            else {
                logger.info("Write lock operation on: " + fromNode.getAddress() + ":" +
                        fromNode.getServerPort() + " succeded.");
                return -1;
            }

        } catch (Exception e) {
            logger.error("MoveData message couldn't be sent to  "
                    + fromNode.getServerPort());
            kvConnection.disconnect();
            return -1;
        }
        return 0;
    }

    /**
     * returns the successor of the newServer
     *
     * @param newServer
     * @return
     */
    private ServerInfo getSuccessor(ServerInfo newServer) {

        ServerInfo successor;
        int nodeIndex = this.activeServers.indexOf(newServer);
        try {
            successor = this.activeServers.get(nodeIndex + 1);
        }// success is the first server on the ring
        catch (IndexOutOfBoundsException e) {
            successor = this.activeServers.get(0);
        }
        return successor;
    }


    /**
     * update metaData to activeServers
     *
     * @return
     */
    private int UpdateMetaData() {
        // send meta data to all active servers
        KVAdminMessageImpl UpdateMsg = new KVAdminMessageImpl();
        UpdateMsg.setStatus(KVAdminMessage.StatusType.UPDATE_METADATA);
        UpdateMsg.setMetadata(activeServers);

        for (ServerInfo server : this.activeServers) {
            byte[] byteMessage;
            KVConnection kvconnection = new KVConnection(server);
            try {
                kvconnection.connect();
                kvconnection.sendMessage(UpdateMsg);
                byteMessage = Utilities.receive(kvconnection.getInput());
                AbstractMessage abstractMessage = Serializer.toObject(byteMessage);
                UpdateMsg = (KVAdminMessageImpl) abstractMessage;
                if (UpdateMsg.getStatus().equals(KVAdminMessage.StatusType.OPERATION_SUCCESS)) {
                    KVConnections.put(server, kvconnection);
                    logger.info("Update Operation on server " + server + " succeeded.");
                    kvconnection.disconnect();
                } else
                    logger.info("Update Operation on server " + server + " failed.");
            } catch (Exception e) {
                kvconnection.disconnect();
                logger.error("Could not send Update Message to server" + server
                        + e.getMessage());
            }
        }
        logger.debug("KV Servers given updated MetaData successfully.");
        return 0;
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
                return o1.getToIndex().compareTo(o2.getToIndex());
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
