package app_kvEcs;

import common.ServerInfo;
import hashing.MD5Hash;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ECSImpl implements ECS {

    private ConfigReader confReader;
    private List<ServerInfo> Servers;
    private MD5Hash md5Hasher;
    private static Logger logger = Logger.getLogger(ECSImpl.class);
    private int cacheSize;
    private String displacementStrategy;


    /**
     *
     */
    public ECSImpl(String fileName) throws IOException {
        try {
            this.confReader = new ConfigReader( fileName );
            Servers = confReader.getServers();
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
    public boolean initService(int numberOfNodes, int cacheSize, String displacementStrategy) { return true; }

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
