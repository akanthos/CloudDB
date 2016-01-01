package app_kvEcs;

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
import java.util.*;


public class ECSHelpers {

    private static Logger logger = Logger.getLogger(ECSHelpers.class);


    /*****************************************************/
    /*   Helper functions for ECS core functionalities   */
    /*****************************************************/

    /**
     *
     * @param startServers
     * @param cacheSize
     * @param displacementStrategy
     * @return
     */
    public List<ServerInfo> launchNodes(List<ServerInfo> activeServers, List<ServerInfo> startServers,
                                         int cacheSize, String displacementStrategy, boolean runLocal, CallRemoteInterface runProcess) {
		/*
		 * it is considered that the invoker and invoked processes are in the
		 * same folder and machine
		 */
        String path = System.getProperty("user.dir");
        String command = "nohup java -jar " + path + "/ms3-server.jar ";

        String arguments[] = new String[4];
        arguments[1] = Integer.toString(cacheSize);
        arguments[2] = displacementStrategy;
        arguments[3] = " ERROR &";
        boolean result;
        Iterator<ServerInfo> iterator = startServers.iterator();
        while (iterator.hasNext()) {
            ServerInfo item = iterator.next();
            arguments[0] = String.valueOf(item.getServerPort());
            if (!runLocal)
                result = runProcess.RunRemoteProcess(item.getAddress(),
                        command, arguments);

            else
                result = runProcess.RunLocalProcess(command, arguments);
            if (result) {
                activeServers.add(item);
                item.setLaunched(true);
                logger.info("Successfully started a server." + item.getID());

            }
            else
                iterator.remove();
        }
        return startServers;
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
    public boolean moveData(ServerInfo fromNode, ServerInfo toNode,
                             Long fromIndex, Long toIndex, TransferType ttype,
                             Map<ServerInfo, KVConnection> KVConnections) {

        // if it is a replicate message no
        // need to lockWrite in the server
        if (ttype == TransferType.MOVE || ttype == TransferType.REMOVE) {
            //KVConnection kvConnection = new KVConnection(fromNode);
            KVAdminMessageImpl lockMsg = new KVAdminMessageImpl();
            lockMsg.setStatus(KVAdminMessage.StatusType.LOCK_WRITE);
            try {
                ECSAction(KVConnections.get(fromNode), fromNode, lockMsg, KVConnections);
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
        else if (ttype == TransferType.RESTORE)
            moveDataMsg.setStatus(KVAdminMessage.StatusType.RESTORE_DATA);
        else if (ttype == TransferType.REMOVE)
            moveDataMsg.setStatus(KVAdminMessage.StatusType.REMOVE_DATA);
        else
            moveDataMsg.setStatus(KVAdminMessage.StatusType.MOVE_DATA);
        moveDataMsg.setLow(fromIndex);
        moveDataMsg.setHigh(toIndex);
        moveDataMsg.setServerInfo(toNode);
        try {
            ECSAction(KVConnections.get(fromNode), fromNode, moveDataMsg, KVConnections);
        } catch (CannotConnectException e) {

            KVConnections.get(fromNode).disconnect();
            logger.info(moveDataMsg.getStatus() + " operation on server " + fromNode.getAddress()+":"
                    + fromNode.getServerPort() + " failed due to Connection problem");
            return false;
        }

        return true;
    }


    /**
     * launch Single Server
     *
     * @param startServer
     * @return 0 in case of successful launch
     */
    public boolean launchNode(List<ServerInfo> activeServers, ServerInfo startServer,
                               int cacheSize, String displacementStrategy, boolean runLocal, CallRemoteInterface runProcess) {

        String path = System.getProperty("user.dir");
        String command = "nohup java -jar " + path + "/ms3-server.jar ";

        String arguments[] = new String[4];
        arguments[0] = String.valueOf(startServer.getServerPort());
        arguments[1] = Integer.toString(cacheSize);
        arguments[2] = displacementStrategy;
        arguments[3] = " ERROR &";
        boolean result;
        //ssh calls
        if (!runLocal)
            result = runProcess.RunRemoteProcess(startServer.getAddress(),
                    command, arguments);
        //local calls
        else
            result = runProcess.RunLocalProcess(command, arguments);
        //store server started successfully
        if (result) {
            activeServers.add(startServer);
            startServer.setLaunched(true);
            return true;
        } else
            return false;
    }

    public boolean ECSAction(KVConnection channel, ServerInfo server, KVAdminMessageImpl message, Map<ServerInfo, KVConnection> KVConnections)
            throws CannotConnectException {
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
    public int getRandom(int size) {
        Random randomGenerator = new Random();
        int randomNum = randomGenerator.nextInt(size);
        logger.info("Picked " + randomNum + " as a random number.");
        return randomNum;
    }

    public void removeFromRing(ServerInfo failedServer, List<ServerInfo> activeServers, Map<ServerInfo, KVConnection> KVConnections) {

        boolean found = false;
        int index = -1;
        Iterator<ServerInfo> it = KVConnections.keySet().iterator();

        for (ServerInfo info : activeServers)
            if (info.getServerPort().equals(failedServer.getServerPort()) && info.getAddress().equals(failedServer.getAddress())) {
                found = true;
                index = activeServers.indexOf(info);
            }
        if (found){
            activeServers.remove(index);
            logger.info("Removed FAILED server: " + failedServer.getAddress() +
                    " : " + failedServer.getServerPort() + " from activeServers List.");
        }
    }

    public void removeFromConnections(ServerInfo failedServer, Map<ServerInfo, KVConnection> KVConnections) {


        for(Iterator<Map.Entry<ServerInfo, KVConnection>> it = KVConnections.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<ServerInfo, KVConnection> entry = it.next();
            if(entry.getKey().getAddress().equals(failedServer.getAddress()) &&
                    entry.getKey().getServerPort().equals(failedServer.getServerPort())) {
                it.remove();
                logger.info("Removed FAILED server: " + failedServer.getAddress() +
                        " : " + failedServer.getServerPort() + " from KVConnections Map.");
            }
        }

    }

    /**
     * Calculate metaData of our ECSInterface system
     *
     * @param servers servers of the current system
     * @return the new metaData
     */
    public List<ServerInfo> generateMetaData(List<ServerInfo> servers, MD5Hash hasher){

        // calculate each server's MD5 Hash value and sort
        // them based on this value

        for (ServerInfo server : servers) {
            long hashKey = hasher.hash(server.getAddress() + ":"
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
     * Create an INIT KVAdminMessage
     * @param startServers
     * @return
     */
    public KVAdminMessageImpl InitMsg(List<ServerInfo> startServers, int cacheSize, String displacementStrategy) {
        KVAdminMessageImpl initMsg = new KVAdminMessageImpl();
        initMsg.setStatus(KVAdminMessage.StatusType.INIT);
        initMsg.setCacheSize(cacheSize);
        initMsg.setDisplacementStrategy(displacementStrategy);
        initMsg.setMetadata(startServers);
        return initMsg;
    }

    /**
     * returns the successor of the newServer
     *
     * @param newServer
     * @return
     */
    public ServerInfo getSuccessor(ServerInfo newServer, List<ServerInfo> activeServers) {

        List<ServerInfo> replicas = Utilities.getReplicas(activeServers, newServer);
        return replicas.get(0);
    }

    /**
     * update metaData to activeServers
     *
     * @return
     */
    public boolean UpdateMetaData(List<ServerInfo> activeServers, Map<ServerInfo, KVConnection> KVConnections) {

        boolean updateSuccess = false;
        // send meta data to all active servers
        KVAdminMessageImpl UpdateMsg = new KVAdminMessageImpl();
        UpdateMsg.setStatus(KVAdminMessage.StatusType.UPDATE_METADATA);
        UpdateMsg.setMetadata(activeServers);

        for (ServerInfo server : activeServers) {
            try {
                if (ECSAction(KVConnections.get(server), server, UpdateMsg, KVConnections))
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

    public void repairSystem(ServerInfo failedServer, List<ServerInfo> activeServers, Map<ServerInfo, KVConnection> KVConnections, ECSCore ecs){

        logger.info("Failure handling regarding server: " + failedServer.getAddress() + ":" + failedServer.getServerPort());
        boolean moveSuccess = true;
        boolean found = false;
        int index = -1;
        List<ServerInfo> replicas = Utilities.getReplicas(activeServers, failedServer);
        List<ServerInfo> coordinators = Utilities.getCoordinators(activeServers, failedServer);
        List<ServerInfo> WriteLockNodes = new ArrayList<>();
        ServerInfo successor = replicas.get(0);
        //only one activeServer and failed
        if (activeServers.size() == 1) {
            logger.error("None of the nodes is alive. No data available :(");
        }
        else if (activeServers.size() < 4){

            logger.debug("Case of failed node in Ring. Starting repairing the system.");
            if (moveData(successor, successor, failedServer.getFromIndex(), failedServer.getToIndex(), TransferType.RESTORE, KVConnections)){
                logger.info("Successfully recover data regarding the failed server.");
                //remove failed server from the ring
                removeFromRing(failedServer, activeServers, KVConnections);
                removeFromConnections(failedServer, KVConnections);
                UpdateMetaData(activeServers, KVConnections);

                // you also have to send the replicated data to each other
                if (activeServers.size() == 2){

                    //replicate data to each other
                    // if success
                    if (moveData(activeServers.get(0), activeServers.get(1), activeServers.get(0).getFromIndex(),
                            activeServers.get(0).getToIndex(), TransferType.REPLICATE, KVConnections)
                            && moveData(activeServers.get(1), activeServers.get(0), activeServers.get(1).getFromIndex(),
                            activeServers.get(1).getToIndex(), TransferType.REPLICATE, KVConnections))
                    {

                        logger.debug("Sent replicated data from "
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
        }
        //more than 3 nodes in the ring
        else{
            logger.debug("Case of failed node appearing in a ring with more than 3 nodes.");

            logger.debug("Replicas size: " + replicas.size());
            logger.debug("Coordinators size: " + coordinators.size());
            logger.debug("Failed server: " + failedServer.getID());
            //if you failed to get the replicated data from the first replica
            //get it from the second
            if (moveData(replicas.get(0), replicas.get(0), failedServer.getFromIndex(), failedServer.getToIndex(), TransferType.RESTORE, KVConnections)){
                logger.info("Data recovered from replica "
                        + replicas.get(0).getAddress() + ":"
                        + replicas.get(0).getServerPort() + " was sent to"
                        + successor.getServerPort());
            }else{
                logger.error("Failed to recover data regarding the failed server from the successor of the failed Node.");
                if (moveData(replicas.get(1), successor, failedServer.getFromIndex(), failedServer.getToIndex(), TransferType.RESTORE, KVConnections)){

                }else {
                    moveSuccess = false;
                    return;
                }
            }
            //send the new me†adata list
            removeFromRing(failedServer, activeServers, KVConnections);
            removeFromConnections(failedServer, KVConnections);

            if (UpdateMetaData(activeServers, KVConnections)){
                logger.info("Case of failure with more than 3 nodes. Successful update.");
            } else{
                logger.info("Case of failure with more than 3 nodes. Failed update.");
            }

            List<ServerInfo> successorReplicas = Utilities.getReplicas(activeServers, replicas.get(0));

            /*
                store data as replication:
                1. from successor of failed Node to replicas
                2. from coordinators to the respective nodes.
             */
            if (moveData(replicas.get(0), successorReplicas.get(0), replicas.get(0).getFromIndex(),
                    replicas.get(0).getToIndex(), TransferType.REPLICATE, KVConnections)){
                logger.debug("Sent data for replication from "
                        + successor.getAddress() + ":" + successor.getServerPort() + " to "
                        + successorReplicas.get(0).getAddress() + ":" + successorReplicas.get(0).getServerPort());
            }
            if (moveData(replicas.get(0), successorReplicas.get(1), replicas.get(0).getFromIndex(),
                    replicas.get(0).getToIndex(), TransferType.REPLICATE, KVConnections)){
                logger.debug("Sent data for replication from "
                        + successor.getAddress() + ":" + successor.getServerPort() + " to "
                        + successorReplicas.get(1).getAddress() + ":" + successorReplicas.get(1).getServerPort());
            }
            if (moveData(coordinators.get(1), replicas.get(1), coordinators.get(1).getFromIndex(),
                    coordinators.get(1).getToIndex(), TransferType.REPLICATE, KVConnections)){
                logger.debug("Sent data for replication from "
                        + successor.getAddress() + ":" + successor.getServerPort() + " to "
                        + successorReplicas.get(0).getAddress() + ":" + successorReplicas.get(0).getServerPort());
            }if (moveData(coordinators.get(0), replicas.get(0), coordinators.get(0).getFromIndex(),
                    coordinators.get(0).getToIndex(), TransferType.REPLICATE, KVConnections)){
                logger.debug("Sent data for replication from "
                        + successor.getAddress() + ":" + successor.getServerPort() + " to "
                        + successorReplicas.get(0).getAddress() + ":" + successorReplicas.get(0).getServerPort());
            }

        }

        //TODO: FIX IT!
        ecs.addNode(10, "FIFO");
        logger.debug("New System state for our system.");
        for (ServerInfo server: activeServers)
            logger.debug(server.getAddress() + ":" + server.getServerPort());


    }

    public boolean replicateData(List<ServerInfo> activeServers, Map<ServerInfo, KVConnection> KVConnections) {


        if (activeServers.size()==1)
            return true;
            //replicate data to each other
        else if (activeServers.size()==2){
            if (moveData(activeServers.get(0), activeServers.get(1), activeServers.get(0).getFromIndex(),
                    activeServers.get(0).getToIndex(), TransferType.REPLICATE, KVConnections)
                    && moveData(activeServers.get(1), activeServers.get(0), activeServers.get(1).getFromIndex(),
                    activeServers.get(1).getToIndex(), TransferType.REPLICATE, KVConnections))
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
                if (moveData(node, replicas.get(0), node.getFromIndex(), node.getToIndex(), TransferType.REPLICATE, KVConnections)
                        && moveData(node, replicas.get(1), node.getFromIndex(), node.getToIndex(), TransferType.REPLICATE, KVConnections))
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

}
