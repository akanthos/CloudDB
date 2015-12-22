package app_kvEcs;


public interface ECSInterface {

    /** Launch servers,
     * Send an INIT message to hand meta-data etc.
     * @param numberOfNodes
     */
    public boolean initService(int numberOfNodes, int cacheSize, String displacementStrategy);

    /**
     * Called by the ECSInterface client starting all the services
     */
    public boolean start();

    /**
     * Called by the ECSInterface client stopping all the services
     */
    public boolean stop();

    /**
     * Called by the ECSInterface client to shut down all the services
     */
    public boolean shutdown();

    /**
     * Adds a node to the ring
     * Update the meta data, and move data operations
     */
    public boolean addNode(int cacheSize, String displacementStrategy);

    /**
     * Removes a node to the ring
     * Update the meta data, and move data operations
     */
    public boolean removeNode();

}
