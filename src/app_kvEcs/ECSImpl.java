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

        boolean initSuccess=false;
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
                if (sendECSCmd(connection, server, initMsg))
                    initSuccess=true;
            } catch (CannotConnectException e) {
                connection.disconnect();
                logger.info(initMsg.getStatus() + " operation on server " + server.getAddress()+":"
                        +server.getServerPort() + " failed due to Connection problem");
                return false;
            }
        }
        return initSuccess;
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
    public boolean start() {

        boolean startSuccess = false;
        KVAdminMessageImpl startMessage = new KVAdminMessageImpl();
        startMessage.setStatus(KVAdminMessage.StatusType.START);
        byte[] byteMessage;

        for (ServerInfo server : this.activeServers) {
            try {
                if (sendECSCmd(KVConnections.get(server), server, startMessage))
                    startSuccess = true;
            } catch (CannotConnectException e) {
                KVConnections.get(server).disconnect();
                logger.info(startMessage.getStatus() + " operation on server " + server.getAddress()+":"
                        +server.getServerPort() + " failed due to Connection problem");
                return false;
            }
        }
        logger.info("Active servers are started.");
        return startSuccess;
    }


    /**
     * Stops the service; all participating KVServers are stopped for
     * processing client requests but the processes remain running.
     * @return true if succeeded else false
     */
    @Override
    public boolean stop() {

        boolean stopSuccess = false;
        KVAdminMessageImpl stopMessage = new KVAdminMessageImpl();
        stopMessage.setStatus(KVAdminMessage.StatusType.STOP);
        KVConnection channel = null;
        byte[] byteMessage;

        for (ServerInfo server : this.activeServers) {
            try {
                if (sendECSCmd(KVConnections.get(server), server, stopMessage))
                    stopSuccess = true;
            } catch (CannotConnectException e) {
                KVConnections.get(server).disconnect();
                logger.info(stopMessage.getStatus() + " operation on server " + server.getAddress()+":"
                        +server.getServerPort() + " failed due to Connection problem");
                return false;
            }
        }
        logger.info("Active servers are started.");
        return stopSuccess;
    }

    /**
     * Stops all server instances and exits the remote processes.
     * @return true if succeeded else false
     */
    @Override
    public boolean shutdown() {

        boolean shutdownSuccess = false;
        KVAdminMessageImpl shutDown = new KVAdminMessageImpl();
        shutDown.setStatus(KVAdminMessage.StatusType.SHUT_DOWN);
        KVConnection channel = null;
        byte[] byteMessage;

        for (ServerInfo server : this.activeServers) {
            try {
                if (sendECSCmd(KVConnections.get(server), server, shutDown))
                    shutdownSuccess = true;
            } catch (CannotConnectException e) {
                KVConnections.get(server).disconnect();
                logger.info(shutDown.getStatus() + " operation on server " + server.getAddress()+":"
                        +server.getServerPort() + " failed due to Connection problem");
                return false;
            }
        }
        this.KVConnections.clear();
        this.activeServers.clear();
        logger.info("Active servers shut down.");
        return shutdownSuccess;
    }


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
    public boolean addNode(int cacheSize, String displacementStrategy){

        boolean addSuccess = false;
        logger.debug("System BEFORE adding new node.");
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
                    return false;
                }
            }
        }
        if (newServer == null) {
            logger.info("No available node to add.");
            return false;
        }
        //calculate the new MetaData.
        activeServers = calculateMetaData(activeServers);

        //initialize the new Server
        KVAdminMessageImpl initMsg = InitMsg(activeServers, cacheSize, displacementStrategy);
        KVConnection kvconnection = new KVConnection(newServer);
        byte[] byteMessage;

        try {
            if (sendECSCmd(kvconnection, newServer, initMsg))
                addSuccess = true;
            else
                return false;
        } catch (CannotConnectException e) {
            kvconnection.disconnect();
            logger.info(initMsg.getStatus() + " operation on server " + newServer.getAddress()+":"
                    + newServer.getServerPort() + " failed due to Connection problem");
            activeServers.remove(newServer);
            KVConnections.remove(kvconnection);
            calculateMetaData(activeServers);
            return false;
        }

        ServerInfo successor = getSuccessor(newServer);
        //tell successor to send data
        if (sendData(successor, newServer, newServer.getFromIndex(), newServer.getToIndex()) == 0)
        {
            //UPDATE METADATA
            if (!UpdateMetaData())
                return false;

            // release the lock from the successor, new Node
            logger.debug("release the lock");
            KVAdminMessageImpl releaseLock = new KVAdminMessageImpl();
            releaseLock.setStatus(KVAdminMessage.StatusType.UNLOCK_WRITE);
            try {
                if (sendECSCmd(KVConnections.get(successor), successor, releaseLock))
                    addSuccess = true;
                else
                    return false;
                logger.debug("All locks are released.");
            } catch (CannotConnectException e) {
                logger.error("Release Lock message couldn't be sent.");
                kvconnection.disconnect();
                return false;
            }


        }
        /*
			 * when move data from successor to the
			 * newNode was not successful
			 */
        else {
            // data could not be moved to the newly added Server
            logger.error("Could not move data from "
                    + successor.getAddress() + ":" + successor.getServerPort() + " to "
                    + newServer.getAddress() + ":" + successor.getServerPort());
            logger.error("Operation addNode Not Successfull.");
            activeServers.remove(newServer);
            kvconnection.disconnect();
            KVConnections.remove(newServer);
            calculateMetaData(activeServers);
        }
        return addSuccess;
    }

    private boolean sendECSCmd(KVConnection channel, ServerInfo server, KVAdminMessageImpl message) throws CannotConnectException {
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
                channel.disconnect();
                return true;
            }else {
                logger.info(message.getStatus() + "Operation on server " + server.getAddress() + ":"
                        + server.getServerPort() + " failed.");
                channel.disconnect();
                return false;
            }
        } catch (IOException e) {
            throw new CannotConnectException(message.getStatus() + " operation on server " + server.getAddress()+":"
                    +server.getServerPort() + " failed due to Connection problem");
        }
    }

    /**
     * Get Random number in range
     *
     * @param size
     *            : the range upper bound
     * @return
     */
    private int pickRandomValue(int size) {
        Random randomGenerator = new Random();
        int randomNum = randomGenerator.nextInt(size);
        logger.info("Picked " + randomNum + " as a random number.");
        return randomNum;
    }

    /**
     * Remove a node from the storage service at an arbitrary position.
     * @return true if succeeded else false
     */
    @Override
    public boolean removeNode() {
        int rmvIndex = pickRandomValue(this.activeServers.size());
        logger.debug("Picked node index to remove " + rmvIndex);
        ServerInfo rmvNode = this.activeServers.get(rmvIndex);
        ServerInfo successor = getSuccessor(rmvNode);
        if (rmvNode.equals(successor)) {
            logger.error("Cannot remove node because it is the only active node available," +
                    " If you want to remove please use the shutdown option");
            return false;
        } else {
            return removeNode(rmvNode);
        }
    }

    public boolean removeNode(ServerInfo deleteNode) {

        //get the successor
        ServerInfo successor = getSuccessor(deleteNode);
        //socket connection of Node to be removed
        KVConnection deleteNodeConnection = this.KVConnections
                .get(deleteNode);
        KVConnection successorConnection = this.KVConnections
                .get(successor);
        //remove NodetoDelete from the active servers list
        //and recalculate the Metadata
        this.activeServers.remove(deleteNode);
        logger.debug("System before removing Node.");
        for (ServerInfo s : activeServers)
            logger.debug(s.getAddress() + ":" + s.getServerPort());
        logger.debug("DONE.");

        //calculate the new MetaData.
        activeServers = calculateMetaData(activeServers);


        if (sendData(deleteNode, successor, deleteNode.getFromIndex(),
                deleteNode.getToIndex()) == 0)
        {
            //UPDATE METADATA
            UpdateMetaData();
            // release the lock from the successor, new Node
            logger.debug("release the lock");
            KVAdminMessageImpl releaseLock = new KVAdminMessageImpl();
            releaseLock.setStatus(KVAdminMessage.StatusType.UNLOCK_WRITE);
            try {
                if (sendECSCmd(KVConnections.get(successor), successor, releaseLock))
                    logger.debug("All locks are released.");
            } catch (CannotConnectException e) {
                deleteNodeConnection.disconnect();
                logger.error("Release Lock message couldn't be sent.");
            }
            deleteNodeConnection.disconnect();
        }
        KVAdminMessageImpl shutDown = new KVAdminMessageImpl();
        shutDown.setStatus(KVAdminMessage.StatusType.SHUT_DOWN);
        try {
            sendECSCmd(deleteNodeConnection, deleteNode, shutDown);
        } catch (CannotConnectException e) {
            logger.error("shut down message couldn't be sent.");
        }

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


        //KVConnection kvConnection = new KVConnection(fromNode);
        KVAdminMessageImpl lockMsg = new KVAdminMessageImpl();
        lockMsg.setStatus(KVAdminMessage.StatusType.LOCK_WRITE);
        try {
            sendECSCmd(KVConnections.get(fromNode), fromNode, lockMsg);
        } catch (CannotConnectException e) {
            KVConnections.get(fromNode).disconnect();
            logger.info(lockMsg.getStatus() + " operation on server " + fromNode.getAddress()+":"
                    + fromNode.getServerPort() + " failed due to Connection problem");
            return -1;
        }

        // send move data message to Successor(fromNode)
        KVAdminMessageImpl moveDataMsg = new KVAdminMessageImpl();
        moveDataMsg.setStatus(KVAdminMessage.StatusType.MOVE_DATA);
        moveDataMsg.setLow(fromIndex);
        moveDataMsg.setHigh(toIndex);
        moveDataMsg.setServerInfo(toNode);
        try {
            sendECSCmd(KVConnections.get(fromNode), fromNode, moveDataMsg);
        } catch (CannotConnectException e) {
            KVConnections.get(fromNode).disconnect();
            logger.info(lockMsg.getStatus() + " operation on server " + fromNode.getAddress()+":"
                    + fromNode.getServerPort() + " failed due to Connection problem");
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
    private boolean UpdateMetaData() {

        boolean updateSuccess = false;
        // send meta data to all active servers
        KVAdminMessageImpl UpdateMsg = new KVAdminMessageImpl();
        UpdateMsg.setStatus(KVAdminMessage.StatusType.UPDATE_METADATA);
        UpdateMsg.setMetadata(activeServers);

        for (ServerInfo server : this.activeServers) {
            try {
                if (sendECSCmd(KVConnections.get(server), server, UpdateMsg))
                    updateSuccess=true;
            } catch (CannotConnectException e) {
                KVConnections.get(server).disconnect();
                logger.info(UpdateMsg.getStatus() + " operation on server " + server.getAddress()+":"
                        +server.getServerPort() + " failed due to Connection problem");
                return false;
            }
        }
        return updateSuccess;
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
