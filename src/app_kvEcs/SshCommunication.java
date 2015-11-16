package app_kvEcs;


public interface SshCommunication {

    /**
     * Sets UserName for the ssh communication
     * @param s Username
     */
    public void setUserName(String s);


    /**
     * Sets Address where the Private key for ssh communication exists
     * @param s Address for the private key
     */
    public void setPrivatekey(String s);

    /**
     * Sets Time-out for the remote process
     * to give feedback (success || fail)
     * @param time
     */
    public void setTimeOut(long time);

    /**
     * invokes the KVserver Process
     * @param host: The address of the remote host
     * @paramm serverPort: The port of the remote host
     * @paramm cacheSize
     * @paramm cachePolicy
     * @paramm ecsPort : Port for communication with ECS
     @return: 0 in case of Success and -1 in case of Failure
     */
    public int invokeRemoteProcess(String host,String command,String[]arguments);

    public int invokeProcessLocally(String command,String[]arguments);

}
