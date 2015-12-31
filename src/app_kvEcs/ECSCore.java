package app_kvEcs;

import common.ServerInfo;
import common.messages.KVAdminMessage;
import common.messages.KVAdminMessageImpl;
import common.utils.Utilities;
import hashing.MD5Hash;
import helpers.CannotConnectException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import app_kvEcs.ECSHelpers.*;

enum TransferType {
    MOVE, REPLICATE, RESTORE, REMOVE
};


public class ECSCore implements ECSInterface {


    private static Logger logger = Logger.getLogger(ECSCore.class);

    private ECSHelpers Helper = new ECSHelpers();
    private List<ServerInfo> allServers;
    private List<ServerInfo> activeServers;
    private ConfigReader confReader;
    private MD5Hash md5Hasher;
    private int cacheSize;
    private String displacementStrategy;
    private boolean runLocal=true;
    private CallRemoteInterface runProcess;
    private FailDetection failHandler;


    private Map<ServerInfo, KVConnection> KVConnections;
    private ServerSocket failSocket;
    private boolean running=false;
    private int detectionPort = 60037;

    /**
     *
     */
    public ECSCore(String fileName) throws IOException {
        try {
            this.confReader = new ConfigReader( fileName );
            allServers = confReader.getServers();
        } catch (IOException e) {
            throw new IOException("ECSCore. Cannot access ecs.config");
        }
    }

    /**
     * Randomly choose <numberOfNodes> servers from the available machines and start the KVServer
     * by issuing a SSH call to the respective machine.
     * This call launches the server with the specified cache size and displacement strategy.
     * You can assume that the KVServer.jar is located in the same directory as the ECSCore.
     * All servers are initialized with the meta-data and remain in state stopped
     * @param numberOfNodes
     * @param cacheSize
     * @param displacementStrategy
     * @return true if succeeded else false
     */
    public boolean initService(int numberOfNodes, int cacheSize, String displacementStrategy) {

        boolean initSuccess=false;
        Random rand = new Random();
        int count = 0;
        this.md5Hasher = new MD5Hash();
        runProcess = new CallRemote();
        this.KVConnections = new HashMap<ServerInfo, KVConnection>();
        running=true;

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

        logger.info("ECSInterface launching " + numberOfNodes + " servers.");
        //start the store servers
        startServers = Helper.launchNodes(this.activeServers, startServers, cacheSize, displacementStrategy, runLocal, runProcess);
        final ECSCore curr=this;

        //failHandler = new FailDetection(50036, curr);
        //new Thread(failHandler).start();

        try {
            failSocket = new ServerSocket();
        } catch (IOException e) {
            logger.error("Failed to create a Socket Server for failure detection.");
        }
        try {
            failSocket.bind(new InetSocketAddress("127.0.0.1", detectionPort));
        } catch (IOException e) {
            logger.error("Failed to bind to Failure detection socket server.");
        }
        new Thread(new Runnable() {
            @Override
            public void run() {
                if (failSocket != null) {
                    while (running) {
                        try {
                            logger.info("Start a thread for failure reporting messages from KVStore servers.");
                            Socket failClient = failSocket.accept();
                            FailDetection connection = new FailDetection(detectionPort, failClient, curr);
                            new Thread(connection).start();
                        } catch (IOException e) {
                            logger.error("Unable to establish connection. \n", e);
                        }
                    }
                }
            }
        }).start();

        ///////////
        //calculate the meta-data => List <ServerInfo>
        startServers = Helper.generateMetaData(startServers, md5Hasher);
        activeServers = startServers;
        // communicate with servers and send call initialize command
        KVAdminMessageImpl initMsg = Helper.InitMsg(activeServers, cacheSize, displacementStrategy);
        // create server connection for further communication with the servers
        for (ServerInfo server : this.activeServers) {
            KVConnection connection = new KVConnection(server);
            try {
                if (Helper.ECSAction(connection, server, initMsg, KVConnections))
                    initSuccess=true;
            } catch (CannotConnectException e) {
                connection.disconnect();
                logger.info(initMsg.getStatus() + " operation on server " + server.getAddress()+":"
                        +server.getServerPort() + " failed due to Connection problem");
                return false;
            }
        }
        Helper.replicateData(activeServers, KVConnections);
        return initSuccess;
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
                if (Helper.ECSAction(KVConnections.get(server), server, startMessage, KVConnections))
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
                if (Helper.ECSAction(KVConnections.get(server), server, stopMessage, KVConnections))
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
                if (Helper.ECSAction(KVConnections.get(server), server, shutDown, KVConnections))
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
        running = false;
        return shutdownSuccess;
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
        ServerInfo newServer = new ServerInfo();
        Iterator<ServerInfo> allServersIterator = allServers.iterator();

        while (allServersIterator.hasNext()) {
            newServer = allServersIterator.next();
            if ( !this.activeServers.contains(newServer) ){
                if (Helper.launchNode(this.activeServers, newServer, cacheSize, displacementStrategy, runLocal, runProcess))
                    break;
                else {
                    logger.warn("Unable to add a new server!");
                    return false;
                }
            }
        }
        if (newServer == null) {
            logger.info("Could not find a server to add.");
            return false;
        }
        logger.info("About to ADD server: " + newServer.getID());
        logger.debug("My system before adding is: ");
        for (ServerInfo server: activeServers)
            logger.debug(server.getID());
        //calculate the new MetaData.
        activeServers = Helper.generateMetaData(activeServers, md5Hasher);
        List<KVConnection> WriteLockNodes = new ArrayList<>();
        List<ServerInfo> replicas = new ArrayList<>();
        List<ServerInfo> coordinators = new ArrayList<>();

        //initialize the new Server
        KVAdminMessageImpl initMsg = Helper.InitMsg(activeServers, cacheSize, displacementStrategy);
        KVConnection kvconnection = new KVConnection(newServer);
        byte[] byteMessage;

        try {
            if (Helper.ECSAction(kvconnection, newServer, initMsg, KVConnections))
                addSuccess = true;
            else
                return false;
        } catch (CannotConnectException e) {
            kvconnection.disconnect();
            logger.info(initMsg.getStatus() + " operation on server " + newServer.getAddress()+":"
                    + newServer.getServerPort() + " failed due to Connection problem");

            Helper.removeFromRing(newServer, activeServers, KVConnections);
            Helper.removeFromConnections(newServer, KVConnections);
            Helper.generateMetaData(activeServers, md5Hasher);
            return false;
        }

        replicas = Utilities.getReplicas(activeServers, newServer);
        coordinators = Utilities.getCoordinators(activeServers, newServer);
        ServerInfo successor = replicas.get(0);
        /**
         * normal case of having more than 2 servers
         * in out Storage System
         */
        if (activeServers.size()>2) {
            logger.info("ADD new server in a ring of size >=3 ");
            // successor sends data to new node
            // coordinators of the new Node send their replicas
            if (Helper.moveData(successor, newServer, newServer.getFromIndex(), newServer.getToIndex(), TransferType.MOVE, KVConnections)
                    && Helper.moveData(coordinators.get(0), newServer, coordinators.get(0).getFromIndex(), coordinators.get(0).getToIndex(),
                    TransferType.REPLICATE, KVConnections)
                    && Helper.moveData(coordinators.get(1), newServer, coordinators.get(1).getFromIndex(), coordinators.get(1).getToIndex(),
                    TransferType.REPLICATE, KVConnections) )
            {

                WriteLockNodes.add(KVConnections.get(successor));
                //
                //WriteLockNodes.add(KVConnections.get(coordinators.get(0)));
                //WriteLockNodes.add(KVConnections.get(coordinators.get(1)));
                if (!Helper.UpdateMetaData(activeServers, KVConnections))
                    return false;

                logger.info("Start sending replicated data to new Server's replicas.");
                if ( Helper.moveData(newServer, replicas.get(0), newServer.getFromIndex(), newServer.getToIndex(), TransferType.REPLICATE, KVConnections)
                        && Helper.moveData(newServer, replicas.get(1), newServer.getFromIndex(), newServer.getToIndex(), TransferType.REPLICATE, KVConnections) ){

                    logger.debug("Success moving data from new Node to his new replicas : "
                            + replicas.get(0).getAddress() + ":" + replicas.get(0).getServerPort() + "   "
                            + replicas.get(1).getAddress() + ":" + replicas.get(1).getServerPort());
                    WriteLockNodes.add(KVConnections.get(newServer));
                    // if number of nodes after adding is greater
                    // equal of 4 we also have to delete some stale replicated data
                    if ( activeServers.size() >=4 ){
                        if (Helper.moveData(Helper.getSuccessor(replicas.get(1), activeServers), Helper.getSuccessor(replicas.get(1), activeServers),
                                newServer.getFromIndex(), newServer.getToIndex(), TransferType.REMOVE, KVConnections)
                            && Helper.moveData(replicas.get(1), replicas.get(1), coordinators.get(1).getFromIndex(),
                                coordinators.get(1).getToIndex(), TransferType.REMOVE, KVConnections)
                            && Helper.moveData(replicas.get(0), replicas.get(0), coordinators.get(0).getFromIndex(),
                                coordinators.get(0).getToIndex(), TransferType.REMOVE, KVConnections))
                        {
                            WriteLockNodes.add(KVConnections.get(replicas.get(1)));
                            WriteLockNodes.add(KVConnections.get(replicas.get(0)));
                            logger.debug("Successfully removed staled replicated data to nodes.");
                        }
                        else{
                            logger.warn("Unsuccessfully removed staled replicated data to nodes.");
                        }

                    }

                }
                else {
                    logger.warn("Replication on system to next two replicas " + replicas.get(0).getAddress() +":"+
                            replicas.get(1).getServerPort() + " and "+
                            replicas.get(1).getAddress() +":"+ replicas.get(1).getServerPort() + " failed.");
                }

                if (!Helper.UpdateMetaData(activeServers, KVConnections)) {
                    logger.error("Failed to update metadata after adding a new Node, update replicas and before" +
                            " relasing the WriteLocks.");
                    return false;
                }

            }
            /*
                 * when move data from successor to the
                 * newNode was not successful÷
                 */
            else {
                // data could not be moved to the newly added Server
                logger.error("Could not move data from "
                        + successor.getAddress() + ":" + successor.getServerPort() + " to "
                        + newServer.getAddress() + ":" + successor.getServerPort());
                logger.error("Operation addNode Not Successfull.");
                Helper.removeFromRing(newServer, activeServers, KVConnections);
                kvconnection.disconnect();
                Helper.removeFromConnections(newServer, KVConnections);
                Helper.generateMetaData(activeServers, md5Hasher);
            }
        }
        // 2 servers in the ring after adding a newNode
        else {
            logger.info("ADD new server in a ring of size <=2 ");
            if (Helper.moveData(successor, newServer, newServer.getFromIndex(), newServer.getToIndex(), TransferType.MOVE, KVConnections)) {
                    WriteLockNodes.add(KVConnections.get(successor));
                logger.debug("Successfully moved data from successor to NewServer added.");
                    if (!Helper.UpdateMetaData(activeServers, KVConnections))
                        return false;

                    //replicate data to each other
                    // if success
                    if ( Helper.moveData(successor, newServer, successor.getFromIndex(), successor.getToIndex(), TransferType.REPLICATE, KVConnections)
                            && Helper.moveData(newServer, successor, newServer.getFromIndex(), newServer.getToIndex(), TransferType.REPLICATE,KVConnections)) {

                        logger.info("Data from successor " + successor.getAddress() + ":" + successor.getServerPort() +
                                " replicated to newly added node " + newServer.getAddress() + ":" + newServer.getServerPort());
                    }
                    //failed to replicate data to each other
                    else {
                        logger.warn("Replication on system with two nodes " + successor.getAddress() + ":" + successor.getServerPort()
                                + " and " + successor.getAddress() + ":" + successor.getServerPort() + " failed.");
                    }
            }
            else {
                    logger.error("Data reallocations from " + successor.getAddress() +":"+ successor.getServerPort() +
                    " to " + newServer.getAddress() +":"+ newServer.getServerPort() + " failed.");
                    Helper.removeFromRing(newServer, activeServers, KVConnections);
                    kvconnection.disconnect();
                    Helper.removeFromConnections(newServer, KVConnections);
                    Helper.generateMetaData(activeServers, md5Hasher);
                    addSuccess = false;
            }

        }
        logger.debug("Time to realise the Write Locks.");
        KVAdminMessageImpl WriteLockRelease = new KVAdminMessageImpl();
        WriteLockRelease.setStatus(KVAdminMessage.StatusType.UNLOCK_WRITE);
        try {
            for (KVConnection connection: WriteLockNodes)
                Helper.ECSAction(connection, connection.getServer(), WriteLockRelease, KVConnections);
            logger.debug("All locks are released.");
        } catch (CannotConnectException e) {
            logger.error("Release Lock message couldn't be sent.");
            kvconnection.disconnect();
            return false;
        }
        logger.info("ADDED server: " + newServer.getID());
        logger.debug("My system after adding is: ");
        for (ServerInfo server: activeServers)
            logger.debug(server.getID());
        return addSuccess;
    }


    /**
     * Remove a node from the storage service at an arbitrary position.
     * @return true if succeeded else false
     */
    @Override
    public boolean removeNode() {
        int rmvIndex = Helper.getRandom(this.activeServers.size());
        logger.debug("Picked node index to remove " + rmvIndex);
        ServerInfo rmvNode = this.activeServers.get(rmvIndex);
        ServerInfo successor = Helper.getSuccessor(rmvNode, activeServers);
        if (rmvNode.equals(successor)) {
            logger.error("Cannot remove node because it is the only active node available," +
                    " If you want to remove please use the shutdown option");
            return false;
        } else {
            return removeNode(rmvNode);
        }
    }

    public boolean removeNode(ServerInfo deleteNode) {

        if (activeServers.size() == 1) {
            logger.info("You have only one Server in the Ring so you cannot remove it.");
            return true;
        }
        logger.info("About to DELETE server: " + deleteNode.getID());
        logger.debug("My system before removing is: ");
        for (ServerInfo server: activeServers)
            logger.debug(server.getID());
        boolean moveSuccess=true;
        //get the successor
        List<ServerInfo> replicas = Utilities.getReplicas(activeServers, deleteNode);
        List<ServerInfo> coordinators = Utilities.getCoordinators(activeServers, deleteNode);
        ServerInfo successor = Helper.getSuccessor(deleteNode, activeServers);
        //socket connection of Node to be removed
        KVConnection deleteNodeConnection = this.KVConnections
                .get(deleteNode);
        KVConnection successorConnection = this.KVConnections
                .get(successor);

        List<KVConnection> WriteLockNodes = new ArrayList<>();

        //remove NodetoDelete from the active servers list
        //and recalculate the Metadata
        Helper.removeFromRing(deleteNode, activeServers, KVConnections);
        logger.debug("System before removing Node.");
        for (ServerInfo s : activeServers)
            logger.debug(s.getAddress() + ":" + s.getServerPort());
        logger.debug("DONE.");

        //calculate the new MetaData.
        activeServers = Helper.generateMetaData(activeServers, md5Hasher);

        if (activeServers.size()>2) {
            //send updated metadata to nodes
            if (!Helper.UpdateMetaData(activeServers, KVConnections))
                return false;
            if (Helper.moveData(deleteNode, successor, deleteNode.getFromIndex(), deleteNode.getToIndex(), TransferType.MOVE, KVConnections)){
                logger.debug("Moved main data from "
                        + deleteNode.getAddress() + ":" + deleteNode.getServerPort() + " to "
                        + successor.getAddress() + ":" + successor.getServerPort());
            }
            else{
                logger.debug("Failed moving main data from "
                        + deleteNode.getAddress() + ":" + deleteNode.getServerPort() + " to "
                        + successor.getAddress() + ":" + successor.getServerPort());
                moveSuccess = false;
            }

            if ( Helper.moveData(coordinators.get(1), replicas.get(0), coordinators.get(1).getFromIndex(),
                    coordinators.get(1).getToIndex(), TransferType.REPLICATE, KVConnections)
                    && Helper.moveData(coordinators.get(0), replicas.get(1), coordinators.get(0).getFromIndex(),
                    coordinators.get(0).getToIndex(), TransferType.REPLICATE, KVConnections))
            {
                logger.debug("Sent replicated Data from "
                        + coordinators.get(1).getAddress() + ":" + coordinators.get(1).getServerPort() + " to "
                        + replicas.get(0).getAddress() + ":" + replicas.get(0).getServerPort());
                logger.debug("Sent replicated Data from "
                        + coordinators.get(0).getAddress() + ":" + coordinators.get(0).getServerPort() + " to "
                        + replicas.get(1).getAddress() + ":" + replicas.get(1).getServerPort());

                logger.debug("Successful move of data between nodes for the" +
                        " case of removing a node in a ring ");
            }
            else{
                moveSuccess = false;
            }


        }
        // we have one or two nodes after removal
        else{
            /**
             * that is sufficient for the case of one remaining node
             */
            if (Helper.UpdateMetaData(activeServers, KVConnections) && Helper.moveData(deleteNode, successor, deleteNode.getFromIndex(),
                    deleteNode.getToIndex(), TransferType.MOVE, KVConnections)) {
                logger.debug("Successfully moved data from Node to be deleted to the successor.");
                WriteLockNodes.add(deleteNodeConnection);

                if (activeServers.size() == 2) {
                    if (Helper.moveData(activeServers.get(0), activeServers.get(1), activeServers.get(0).getFromIndex(),
                            activeServers.get(0).getToIndex(), TransferType.REPLICATE, KVConnections)
                            && Helper.moveData(activeServers.get(1), activeServers.get(0), activeServers.get(1).getFromIndex(),
                            activeServers.get(1).getToIndex(), TransferType.REPLICATE, KVConnections))
                    {

                        logger.debug("Sent replicated Data from "
                                + activeServers.get(0).getAddress() + ":" + activeServers.get(0).getServerPort() + " to "
                                + activeServers.get(1).getAddress() + ":" + activeServers.get(1).getServerPort());
                        logger.debug("Sent replicated Data from "
                                + activeServers.get(1).getAddress() + ":" + activeServers.get(1).getServerPort() + " to "
                                + activeServers.get(0).getAddress() + ":" + activeServers.get(0).getServerPort());

                        logger.debug("Successful move of data between nodes for the" +
                                " case of removing a node in a ring ");

                    }
                    else{
                        moveSuccess = false;
                    }
                }
            }
            else{
                moveSuccess = false;
            }
        }

        //failed to move || replicate data
        if (!moveSuccess){
            logger.warn("Failed to move or replicate data.");
            logger.error("Get back to the previous state.");
            activeServers.add(deleteNode);
            KVConnections.put(deleteNode, deleteNodeConnection);
            Helper.UpdateMetaData(activeServers, KVConnections);

        }
        // release the lock from the successor, new Node
        logger.debug("release the lock");
        KVAdminMessageImpl WriteLockRelease = new KVAdminMessageImpl();
        WriteLockRelease.setStatus(KVAdminMessage.StatusType.UNLOCK_WRITE);
        try {
            if (Helper.ECSAction(KVConnections.get(successor), successor, WriteLockRelease, KVConnections))
                logger.debug("All locks are released.");
        } catch (CannotConnectException e) {
            deleteNodeConnection.disconnect();
            logger.error("Release Lock message couldn't be sent.");
        }
        deleteNodeConnection.disconnect();
        KVAdminMessageImpl shutDown = new KVAdminMessageImpl();
        shutDown.setStatus(KVAdminMessage.StatusType.SHUT_DOWN);
        try {
            Helper.ECSAction(deleteNodeConnection, deleteNode, shutDown, KVConnections);
        } catch (CannotConnectException e) {
            logger.error("shut down message couldn't be sent.");
        }

        logger.info("DELETED server: " + deleteNode.getID());
        logger.debug("My system after removing is: ");
        for (ServerInfo server: activeServers)
            logger.debug(server.getID());

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

    public synchronized void handleFailure(ServerInfo failNode) {

        for (ServerInfo s : activeServers) {
            if (s.getID().equals(failNode.getID())) {
                Helper.repairSystem(failNode, activeServers, KVConnections, this);
                break;
            }
        }
    }

    public int getSize(){
        return activeServers.size();
    }

    public List<ServerInfo> getActiveServers() {
        return activeServers;
    }

    public Map<ServerInfo, KVConnection> getKVConnections() {
        return KVConnections;
    }

    /**
     * Lock the KVServer for write operations.
     * @return true if succeeded else false
     */
    public boolean lockWrite(){
        //KVConnection kvConnection = new KVConnection(fromNode);
        KVAdminMessageImpl lockMsg = new KVAdminMessageImpl();
        lockMsg.setStatus(KVAdminMessage.StatusType.LOCK_WRITE);
        boolean allDone = false;

        for (ServerInfo server: activeServers) {
            try {
                if (!Helper.ECSAction(KVConnections.get(server), server, lockMsg, KVConnections))
                    return false;

            } catch (CannotConnectException e) {
                KVConnections.get(server).disconnect();
                logger.info(lockMsg.getStatus() + " operation on server " + server.getAddress() + ":"
                        + server.getServerPort() + " failed due to Connection problem");
                return false;
            }
        }
        return true;
    }

    /**
     * Unlock the KVServer for write operations.
     * @return true if succeeded else false
     */
    public boolean unlockWrite( ){
        //KVConnection kvConnection = new KVConnection(fromNode);
        KVAdminMessageImpl lockMsg = new KVAdminMessageImpl();
        lockMsg.setStatus(KVAdminMessage.StatusType.UNLOCK_WRITE);
        boolean allDone = false;

        for (ServerInfo server: activeServers) {
            try {
                if (!Helper.ECSAction(KVConnections.get(server), server, lockMsg, KVConnections))
                    return false;

            } catch (CannotConnectException e) {
                KVConnections.get(server).disconnect();
                logger.info(lockMsg.getStatus() + " operation on server " + server.getAddress() + ":"
                        + server.getServerPort() + " failed due to Connection problem");
                return false;
            }
        }
        return true;
    }

    /**
     * Get an active server
     * @return
     */
    public ServerInfo getEntryServer(){
        int serverIndex = Helper.getRandom(activeServers.size());
        ServerInfo entryNode = activeServers.get(serverIndex);
        return  entryNode;
    }
}
