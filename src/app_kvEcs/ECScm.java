package app_kvEcs;

import com.javafx.tools.doclets.internal.toolkit.util.Util;
import common.Serializer;
import common.ServerInfo;
import common.messages.AbstractMessage;
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

public class ECScm implements ECSInterface {


    public enum TransferType {
        MOVE, REPLICATE, RESTORE
    };

    private static Logger logger = Logger.getLogger(ECScm.class);
    private List<ServerInfo> allServers;
    private List<ServerInfo> activeServers;
    private ConfigReader confReader;
    private MD5Hash md5Hasher;
    private int cacheSize;
    private String displacementStrategy;
    private boolean runLocal=true;
    private SshCommunication runProcess;
    private FailDetection failHandler;
    private Map<ServerInfo, KVConnection> KVConnections;
    private ServerSocket failSocket;
    private boolean running=false;
    private int detectionPort = 60036;

    /**
     *
     */
    public ECScm(String fileName) throws IOException {
        try {
            this.confReader = new ConfigReader( fileName );
            allServers = confReader.getServers();
        } catch (IOException e) {
            throw new IOException("ECScm. Cannot access ecs.config");
        }
    }

    /**
     * Randomly choose <numberOfNodes> servers from the available machines and start the KVServer
     * by issuing a SSH call to the respective machine.
     * This call launches the server with the specified cache size and displacement strategy.
     * You can assume that the KVServer.jar is located in the same directory as the ECScm.
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
        runProcess = new SshCaller();
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
        startServers = launchNodes(startServers, cacheSize, displacementStrategy);
        final ECScm curr=this;

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
                            logger.debug("Start thread for failure reporting");
                            Socket failClient = failSocket.accept();
                            FailDetection connection = new FailDetection(detectionPort, failSocket, curr);
                            new Thread(connection).start();
                        } catch (IOException e) {
                            logger.error("Unable to establish connection. \n", e);
                        }
                    }
                }
                logger.info("Stopped server.");
            }
        }).start();

        ///////////
        //calculate the meta-data => List <ServerInfo>
        startServers = generateMetaData(startServers);
        activeServers = startServers;
        // communicate with servers and send call initialize command
        KVAdminMessageImpl initMsg = InitMsg(activeServers, cacheSize, displacementStrategy);
        // create server connection for further communication with the servers
        for (ServerInfo server : this.activeServers) {
            KVConnection connection = new KVConnection(server);
            try {
                if (ECSAction(connection, server, initMsg))
                    initSuccess=true;
            } catch (CannotConnectException e) {
                connection.disconnect();
                logger.info(initMsg.getStatus() + " operation on server " + server.getAddress()+":"
                        +server.getServerPort() + " failed due to Connection problem");
                return false;
            }
        }
        // TODO: Send message to Server to Replicate their data?
        replicateData();
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
            if (!runLocal)
                result = runProcess.RunRemoteProcess(item.getAddress(),
                        command, arguments);

            else
                result = runProcess.RunLocalProcess(command, arguments);
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
                if (ECSAction(KVConnections.get(server), server, startMessage))
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
                if (ECSAction(KVConnections.get(server), server, stopMessage))
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
                if (ECSAction(KVConnections.get(server), server, shutDown))
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
        if (!runLocal)
            result = runProcess.RunRemoteProcess(startServer.getAddress(),
                    command, arguments);
        // for runLocal invocations
        else
            result = runProcess.RunLocalProcess(command, arguments);

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
        activeServers = generateMetaData(activeServers);
        List<KVConnection> WriteLockNodes = new ArrayList<>();
        List<ServerInfo> replicas = new ArrayList<>();
        List<ServerInfo> coordinators = new ArrayList<>();

        //initialize the new Server
        KVAdminMessageImpl initMsg = InitMsg(activeServers, cacheSize, displacementStrategy);
        KVConnection kvconnection = new KVConnection(newServer);
        byte[] byteMessage;

        try {
            if (ECSAction(kvconnection, newServer, initMsg))
                addSuccess = true;
            else
                return false;
        } catch (CannotConnectException e) {
            kvconnection.disconnect();
            logger.info(initMsg.getStatus() + " operation on server " + newServer.getAddress()+":"
                    + newServer.getServerPort() + " failed due to Connection problem");
            activeServers.remove(newServer);
            KVConnections.remove(kvconnection);
            generateMetaData(activeServers);
            return false;
        }
        ServerInfo successor = getSuccessor(newServer);
        replicas = Utilities.getReplicas(activeServers, newServer);
        coordinators = Utilities.getCoordinators(activeServers, newServer);

        /**
         * normal case of having more than 2 servers
         * in out Storage System
         */
        if (activeServers.size()>2) {
            // successor sends data to new node
            // coordinators of the new Node send their replicas
            if (moveData(successor, newServer, newServer.getFromIndex(), newServer.getToIndex(), TransferType.MOVE)
                    && moveData(coordinators.get(0), newServer, coordinators.get(0).getFromIndex(), coordinators.get(0).getToIndex(), TransferType.REPLICATE)
                    && moveData(coordinators.get(1), newServer, coordinators.get(1).getFromIndex(), coordinators.get(1).getToIndex(), TransferType.REPLICATE) )
            {

                WriteLockNodes.add(KVConnections.get(successor));
                //
                //WriteLockNodes.add(KVConnections.get(coordinators.get(0)));
                //WriteLockNodes.add(KVConnections.get(coordinators.get(1)));
                if (!UpdateMetaData())
                    return false;

                logger.info("Start sending replicated data to new Server's replicas.");
                if ( moveData(newServer, replicas.get(0), newServer.getFromIndex(), newServer.getToIndex(), TransferType.REPLICATE)
                        && moveData(newServer, replicas.get(1), newServer.getFromIndex(), newServer.getToIndex(), TransferType.REPLICATE) ){

                    logger.debug("Success moving data from new Node to his new replicas : "
                            + replicas.get(0).getAddress() + ":" + replicas.get(0).getServerPort() + "   "
                            + replicas.get(1).getAddress() + ":" + replicas.get(1).getServerPort());
                    WriteLockNodes.add(KVConnections.get(newServer));
                    // if number of nodes after adding is greater
                    // equal of 4 we also have to delete some stale replicated data
                    if ( activeServers.size() >=4 ){
                        if (deleteData(getSuccessor(replicas.get(1)), newServer.getFromIndex(), newServer.getToIndex())
                               && deleteData(replicas.get(1), coordinators.get(1).getFromIndex(), coordinators.get(1).getToIndex())
                                && deleteData(replicas.get(0), coordinators.get(0).getFromIndex(), coordinators.get(0).getToIndex())){

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

                if (!UpdateMetaData()) {
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
                activeServers.remove(newServer);
                kvconnection.disconnect();
                KVConnections.remove(newServer);
                generateMetaData(activeServers);
            }
        }
        // 2 servers in the ring after adding a newNode
        else {
            if (moveData(successor, newServer, newServer.getFromIndex(), newServer.getToIndex(), TransferType.MOVE)) {
                    WriteLockNodes.add(KVConnections.get(successor));
                logger.debug("Successfully moved data from successor to NewServer added.");
                    if (!UpdateMetaData())
                        return false;

                    //replicate data to each other
                    // if success
                    if ( moveData(successor, newServer, successor.getFromIndex(), successor.getToIndex(), TransferType.REPLICATE)
                            && moveData(newServer, successor, newServer.getFromIndex(), newServer.getToIndex(), TransferType.REPLICATE)) {

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
                    activeServers.remove(newServer);
                    kvconnection.disconnect();
                    KVConnections.remove(newServer);
                    generateMetaData(activeServers);
                    addSuccess = false;
            }

        }
        logger.debug("Time to realise the Write Locks.");
        KVAdminMessageImpl WriteLockRelease = new KVAdminMessageImpl();
        WriteLockRelease.setStatus(KVAdminMessage.StatusType.UNLOCK_WRITE);
        try {
            for (KVConnection connection: WriteLockNodes)
                ECSAction(connection, connection.getServer(), WriteLockRelease);
            logger.debug("All locks are released.");
        } catch (CannotConnectException e) {
            logger.error("Release Lock message couldn't be sent.");
            kvconnection.disconnect();
            return false;
        }

        return addSuccess;
    }



    public void repairSystem(ServerInfo failedServer){

        boolean moveSuccess = true;
        List<ServerInfo> WriteLockNodes = new ArrayList<>();
        ServerInfo successor = getSuccessor(failedServer);
        //only one activeServer and failed
        if (activeServers.size() == 1) {
            logger.error("None of the nodes is alive. No data available :(");
        }
        else if (activeServers.size() < 4){

            logger.debug("Case of failed node in Ring. Starting repairing the system.");
            if (moveData(failedServer, failedServer, failedServer.getFromIndex(), failedServer.getToIndex(), TransferType.RESTORE)){
                logger.info("Successfully recover data regarding the failed server.");
                //remove failed server from the ring
                activeServers.remove(failedServer);
                KVConnections.remove(failedServer);
                UpdateMetaData();

                // you also have to send the replicated data to each other
                if (activeServers.size() == 3){

                    //replicate data to each other
                    // if success
                    if (moveData(activeServers.get(0), activeServers.get(1), activeServers.get(0).getFromIndex(),
                            activeServers.get(0).getToIndex(), TransferType.REPLICATE)
                            && moveData(activeServers.get(1), activeServers.get(0), activeServers.get(1).getFromIndex(),
                            activeServers.get(1).getToIndex(), TransferType.REPLICATE))
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

                logger.debug("System repaired.");
            }
            else{
                logger.error("Failed to recover data regarding the failed server.");
                moveSuccess = false;
            }
            logger.debug("I have a system with 3 nodes and one of them failed.");
            //send message to successor to save replicated
            //data from the failed node as his main data
        }
        //more than 3 nodes in the ring
        else{
            logger.debug("Case of failed node appearing in a ring with more than 3 nodes.");
            List<ServerInfo> replicas = Utilities.getReplicas(activeServers, failedServer);
            List<ServerInfo> coordinators = Utilities.getCoordinators(activeServers, failedServer);

            //if you failed to get the replicated data from the first replica
            //get it from the second
            if (moveData(failedServer, failedServer, failedServer.getFromIndex(), failedServer.getToIndex(), TransferType.RESTORE)){
                logger.info("Data recovered from replica "
                        + replicas.get(1).getAddress() + ":"
                        + replicas.get(1).getServerPort() + " was sent to"
                        + successor.getServerPort());
            }else{
                logger.error("Failed to recover data regarding the failed server from the successor of the failed Node.");
                if (moveData(replicas.get(1), successor, failedServer.getFromIndex(), failedServer.getToIndex(), TransferType.RESTORE)){

                }else {
                    moveSuccess = false;
                    return;
                }
            }
            //send the new me†adata list
            activeServers.remove(failedServer);
            KVConnections.remove(failedServer);

            if (UpdateMetaData()){
                logger.info("Case of failure with more than 3 nodes. Successful update.");
            } else{
                logger.info("Case of failure with more than 3 nodes. Failed update.");
            }

            List<ServerInfo> successorReplicas = Utilities.getReplicas(activeServers, successor);

            /*
                store data as replication:
                1. from successor of failed Node to replicas
                2. from coordinators to the respective nodes.
             */
            if (moveData(successor, successorReplicas.get(0), successor.getFromIndex(), successor.getToIndex(), TransferType.REPLICATE)){
                logger.debug("Sent data for replication from "
                        + successor.getAddress() + ":" + successor.getServerPort() + " to "
                        + successorReplicas.get(0).getAddress() + ":" + successorReplicas.get(0).getServerPort());
            }
            if (moveData(successor, successorReplicas.get(1), successor.getFromIndex(), successor.getToIndex(), TransferType.REPLICATE)){
                logger.debug("Sent data for replication from "
                        + successor.getAddress() + ":" + successor.getServerPort() + " to "
                        + successorReplicas.get(1).getAddress() + ":" + successorReplicas.get(1).getServerPort());
            }
            if (moveData(coordinators.get(1), replicas.get(1), coordinators.get(1).getFromIndex(), coordinators.get(1).getToIndex(), TransferType.REPLICATE)){
                logger.debug("Sent data for replication from "
                        + successor.getAddress() + ":" + successor.getServerPort() + " to "
                        + successorReplicas.get(0).getAddress() + ":" + successorReplicas.get(0).getServerPort());
            }if (moveData(coordinators.get(0), replicas.get(0), coordinators.get(0).getFromIndex(), coordinators.get(0).getToIndex(), TransferType.REPLICATE)){
                logger.debug("Sent data for replication from "
                        + successor.getAddress() + ":" + successor.getServerPort() + " to "
                        + successorReplicas.get(0).getAddress() + ":" + successorReplicas.get(0).getServerPort());
            }

        }

        //TODO: FIX IT!
        addNode(10, "FIFO");
        logger.debug("New System state for our system.");
        for (ServerInfo server: activeServers)
            logger.debug(server.getAddress() + ":" + server.getServerPort());


    }


    private boolean ECSAction(KVConnection channel, ServerInfo server, KVAdminMessageImpl message) throws CannotConnectException {
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
    private int getRandom(int size) {
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
        int rmvIndex = getRandom(this.activeServers.size());
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


        boolean moveSuccess=true;
        //get the successor
        ServerInfo successor = getSuccessor(deleteNode);
        //socket connection of Node to be removed
        KVConnection deleteNodeConnection = this.KVConnections
                .get(deleteNode);
        KVConnection successorConnection = this.KVConnections
                .get(successor);
        List<ServerInfo> replicas = new ArrayList<>();
        List<ServerInfo> coordinators = new ArrayList<>();
        List<KVConnection> WriteLockNodes = new ArrayList<>();

        //remove NodetoDelete from the active servers list
        //and recalculate the Metadata
        this.activeServers.remove(deleteNode);
        logger.debug("System before removing Node.");
        for (ServerInfo s : activeServers)
            logger.debug(s.getAddress() + ":" + s.getServerPort());
        logger.debug("DONE.");

        //calculate the new MetaData.
        activeServers = generateMetaData(activeServers);

        if (activeServers.size()>2) {
            //send updated metadata to nodes
            if (!UpdateMetaData())
                return false;
            if (moveData(deleteNode, successor, deleteNode.getFromIndex(), deleteNode.getToIndex(), TransferType.MOVE)){
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

            if ( moveData(coordinators.get(1), replicas.get(0), coordinators.get(1).getFromIndex(),
                    coordinators.get(1).getToIndex(), TransferType.REPLICATE)
                    && moveData(coordinators.get(0), replicas.get(1), coordinators.get(0).getFromIndex(),
                    coordinators.get(0).getToIndex(), TransferType.REPLICATE))
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
            if (UpdateMetaData() && moveData(deleteNode, successor, deleteNode.getFromIndex(), deleteNode.getToIndex(), TransferType.MOVE)) {
                logger.debug("Successfully moved data from Node to be deleted to the successor.");
                WriteLockNodes.add(deleteNodeConnection);

                if (activeServers.size() == 2) {
                    if (moveData(activeServers.get(0), activeServers.get(1), activeServers.get(0).getFromIndex(),
                            activeServers.get(0).getToIndex(), TransferType.REPLICATE)
                            && moveData(activeServers.get(1), activeServers.get(0), activeServers.get(1).getFromIndex(),
                            activeServers.get(1).getToIndex(), TransferType.REPLICATE))
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
            UpdateMetaData();

        }
        // release the lock from the successor, new Node
        logger.debug("release the lock");
        KVAdminMessageImpl WriteLockRelease = new KVAdminMessageImpl();
        WriteLockRelease.setStatus(KVAdminMessage.StatusType.UNLOCK_WRITE);
        try {
            if (ECSAction(KVConnections.get(successor), successor, WriteLockRelease))
                logger.debug("All locks are released.");
        } catch (CannotConnectException e) {
            deleteNodeConnection.disconnect();
            logger.error("Release Lock message couldn't be sent.");
        }
        deleteNodeConnection.disconnect();
        KVAdminMessageImpl shutDown = new KVAdminMessageImpl();
        shutDown.setStatus(KVAdminMessage.StatusType.SHUT_DOWN);
        try {
            ECSAction(deleteNodeConnection, deleteNode, shutDown);
        } catch (CannotConnectException e) {
            logger.error("shut down message couldn't be sent.");
        }

        return true;
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
                if (!ECSAction(KVConnections.get(server), server, lockMsg))
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
                if (!ECSAction(KVConnections.get(server), server, lockMsg))
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
    private boolean moveData(ServerInfo fromNode, ServerInfo toNode,
                         Long fromIndex, Long toIndex, TransferType ttype) {

        // if it is a replicate message no
        // need to lockWrite in the server
        if (ttype == TransferType.MOVE) {
            //KVConnection kvConnection = new KVConnection(fromNode);
            KVAdminMessageImpl lockMsg = new KVAdminMessageImpl();
            lockMsg.setStatus(KVAdminMessage.StatusType.LOCK_WRITE);
            try {
                ECSAction(KVConnections.get(fromNode), fromNode, lockMsg);
            } catch (CannotConnectException e) {
                KVConnections.get(fromNode).disconnect();
                logger.info(lockMsg.getStatus() + " operation on server " + fromNode.getAddress() + ":"
                        + fromNode.getServerPort() + " failed due to Connection problem");
                return false;
            }
        }
        KVAdminMessageImpl moveDataMsg = new KVAdminMessageImpl();
        if (ttype == TransferType.REPLICATE)
            moveDataMsg.setStatus(KVAdminMessage.StatusType.REPLICATE_DATA);
        else
            moveDataMsg.setStatus(KVAdminMessage.StatusType.MOVE_DATA);
        moveDataMsg.setLow(fromIndex);
        moveDataMsg.setHigh(toIndex);
        moveDataMsg.setServerInfo(toNode);
        try {
            ECSAction(KVConnections.get(fromNode), fromNode, moveDataMsg);
        } catch (CannotConnectException e) {

            KVConnections.get(fromNode).disconnect();
            logger.info(moveDataMsg.getStatus() + " operation on server " + fromNode.getAddress()+":"
                    + fromNode.getServerPort() + " failed due to Connection problem");
            return false;
        }

        return true;
    }


    private boolean replicateData() {


        if (activeServers.size()==1)
            return true;
        //replicate data to each other
        else if (activeServers.size()==2){
            if (moveData(activeServers.get(0), activeServers.get(1), activeServers.get(0).getFromIndex(),
                    activeServers.get(0).getToIndex(), TransferType.REPLICATE)
                    && moveData(activeServers.get(1), activeServers.get(0), activeServers.get(1).getFromIndex(),
                    activeServers.get(1).getToIndex(), TransferType.REPLICATE))
            {

                logger.debug("Sent replicated Data from "
                        + activeServers.get(0).getAddress() + ":" + activeServers.get(0).getServerPort() + " to "
                        + activeServers.get(1).getAddress() + ":" + activeServers.get(1).getServerPort());
                logger.debug("Sent replicated Data from "
                        + activeServers.get(1).getAddress() + ":" + activeServers.get(1).getServerPort() + " to "
                        + activeServers.get(0).getAddress() + ":" + activeServers.get(0).getServerPort());

                logger.debug("Successful replication process in KVStore System/Ring.");
                return true;
            }
            else{
                logger.warn("UnSuccessful replication process in KVStore System/Ring.");
                return false;
            }
        }
        //more than 2 nodes in the ring.
        //All servers have to replicas (Normal-general case)
        else{
            for (ServerInfo node : activeServers ){
                List<ServerInfo> replicas = Utilities.getReplicas(activeServers, node);
                if (moveData(node, replicas.get(0), node.getFromIndex(), node.getToIndex(), TransferType.REPLICATE)
                    && moveData(node, replicas.get(1), node.getFromIndex(), node.getToIndex(), TransferType.REPLICATE))
                    continue;
                else{
                    logger.warn("Unsuccessful senting replicated Data from "
                            + node.getAddress() + ":" + node.getServerPort());
                    logger.warn("UnSuccessful replication process in KVStore System/Ring with more than 2 Nodes.");

                }
            }
            logger.debug("Successful replication process in KVStore System/Ring.");
            return true;
        }
    }


    private boolean deleteData(ServerInfo node,
                               Long fromIndex, Long toIndex){

        KVAdminMessageImpl deleteDataMsg = new KVAdminMessageImpl();
        deleteDataMsg.setStatus(KVAdminMessage.StatusType.REMOVE_DATA);
        deleteDataMsg.setLow(fromIndex);
        deleteDataMsg.setHigh(toIndex);
        try {
            ECSAction(KVConnections.get(node), node, deleteDataMsg);
        } catch (CannotConnectException e) {
            KVConnections.get(node).disconnect();
            logger.info(deleteDataMsg.getStatus() + " operation on server " + node.getAddress()+":"
                    + node.getServerPort() + " failed due to Connection problem");
            return false;
        }
        return true;

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
                if (ECSAction(KVConnections.get(server), server, UpdateMsg))
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
     * Calculate metaData of our ECSInterface system
     *
     * @param servers servers of the current system
     * @return the new metaData
     */
    private List<ServerInfo> generateMetaData(List<ServerInfo> servers){

        // calculate each server's MD5 Hash value and sort
        // them based on this value

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

        // set the predecessor
        for (int i = 0; i < servers.size(); i++) {
            ServerInfo server = servers.get(i);
            ServerInfo predecessor;
            if (i == 0) {
                // case of fist node
                predecessor = servers.get(servers.size() - 1);
            } else {
                predecessor = servers.get(i - 1);
            }
            server.setFromIndex(predecessor.getToIndex());
        }
        return servers;
    }

    /**
     * Get an active server
     * @return
     */
    public ServerInfo getEntryServer(){
        int serverIndex = getRandom(this.activeServers.size());
        ServerInfo entryNode = this.activeServers.get(serverIndex);
        return  entryNode;
    }


    public synchronized void handleFailure(ServerInfo failNode) {

        logger.info("Failure delivered regarding server: "+ failNode.getAddress()+":"+ failNode.getServerPort());
        repairSystem(failNode);
    }


}
