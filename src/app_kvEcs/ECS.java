package app_kvEcs;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class ECS {

    private ConfigReader confReader;
    private List<ServerInfos> Servers;
    private static Logger logger = Logger.getLogger(ECS.class);
    private int cacheSize;
    private String displacementStrategy;


    /**
     *
     */
    public ECS (String fileName) throws IOException {
        try {
            this.confReader = new ConfigReader( fileName );
            Servers = confReader.getServers();
        } catch (IOException e) {
            throw new IOException("ECS. Cannot access ecs.config");
        }
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
     * Randomly choose <numberOfNodes> servers from the available machines and start the KVServer
     * by issuing a SSH call to the respective machine.
     * This call launches the server with the specified cache size and displacement strategy.
     * You can assume that the KVServer.jar is located in the same directory as the ECS.
     * All servers are initialized with the meta-data and remain in state stopped
     * @param numberOfNodes
     * @param cacheSize
     * @param displacementStrategy
     * @return true if succeeded else false
     */
    public boolean initService(int numberOfNodes, int cacheSize, String displacementStrategy) { return true; }

    /**
     * Create a new KVServer with the specified cache size and displacement strategy
     * and add it to the storage service at an arbitrary position.
     * @param cacheSize
     * @param displacementStrategy
     * @return true if succeeded else false
     */
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
    public boolean lockWrite( ServerInfos server){
        return true;
    }

    /**
     * Unlock the KVServer for write operations.
     * @return true if succeeded else false
     */
    public boolean unlockWrite( ServerInfos server){
        return true;
    }


}
