package app_kvEcs;


public interface ECS {

    /** Send an SSH call to launch number of servers,
     * Send an INIT message to hand meta data.
     * @param numberOfNodes
     */
    public boolean initService(int numberOfNodes, int cacheSize, String displacementStrategy);

    /**
     * Called by the ECS client starting all the services
     */
    public void start();

    /**
     * Called by the ECS client stopping all the services
     */
    public void stop();

    /**
     * Called by the ECS client to shut down all the services
     */
    public boolean shutdown();

    /**
     * Adds a node to the ring
     * Update the meta data, and move data operations
     */
    public void addNode(int cacheSize, String displacementStrategy);

    /**
     * Removes a node to the ring
     * Update the meta data, and move data operations
     */
    public boolean removeNode();

}
