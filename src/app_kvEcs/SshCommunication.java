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
     * @paramm ecsPort : Port for communication with ECS
     @return: 0 in case of Success and -1 in case of Failure
     */
    public int RunRemoteProcess(String host, String command, String[] arguments);

    /**
     * Called to invoke local call without using SSH
     *
     * @param command
     * @param arguments
     * @return
     */
    @Override
    public int RunLocalProcess(String command, String[] arguments) {

        //ERROR & -> dont write standard error in nohup.out
        try {
            // adding the arguments to the command
            for (String argument : arguments)
                command += " " + argument;
            ProcessBuilder processb = new ProcessBuilder("nohup", "java", "-jar", "ms3-server.jar", arguments[0], "&");
            String path = System.getProperty("user.dir");

            processb.directory(new File(path));
            Process p = processb.start();
            InputStream in = p.getInputStream();
            long begin = System.currentTimeMillis();
            long end = begin + timeOut;
            String s = "";
            char c;
            boolean waiting = true;
            while (System.currentTimeMillis() < end && waiting) {
                while (in.available() > 0) {
                    c = (char) in.read();
                    s += c;
                    if (s.contains("SUCCESS")) {
                        waiting = false;
                        break;
                    }
                    // the process did not start successfully
                    if (s.contains("ERROR"))
                        return -1;
                    // no more input to read
                    if ((int) c < 0)
                        break;
                }

            }
            logger.info("Local Server" + " has started! Listening on Port "
                    + arguments[0]);

            return 0;
        } catch (Exception e) {
            System.out.println(e);
            logger.error(e);
            return -1;
        }

    }

}
