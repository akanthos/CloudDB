package app_kvEcs;

import org.apache.log4j.Logger;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import java.io.InputStream;


public class SshCaller implements SshCommunication {

    public static Logger logger = Logger.getRootLogger();
    private String userName = System.getProperty("user.name");
    private String privateKey = System.getProperty("user.home") + "/.ssh/id_rsa";
    private String knownHosts = System.getProperty("user.home") + "/.ssh/known_hosts";
    private static final int port = 22;
    private long timeOut = 3000;

    @Override
    public void setUserName(String s) {
        this.userName = s;
    }

    @Override
    public void setPrivatekey(String s) {
        this.privateKey = s;
    }

    @Override
    public void setTimeOut(long time) {
        this.timeOut = time;
    }

    /**
     *
     * Called to invoke remote processes through SSH
     *
     * @param host: The address of the remote host
     * @param command Remote command to be executed
     * @param arguments Required arguments (check function's declaration Interface)
     * @return
     */
    @Override
    public int invokeRemoteProcess(String host, String command, String[] arguments) {

        boolean waiting;
        String tmpResponse = "";
        char c;

        try {
            JSch jsch = new JSch();
            jsch.setKnownHosts(knownHosts);
            Session session = jsch.getSession(userName, host, port);
            jsch.addIdentity(privateKey);
            session.connect(3000);
            // Add arguments to exec Command
            for (String argument : arguments)
                command += " " + argument;

            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            channel.setInputStream(null);
            InputStream in = channel.getInputStream();
            channel.connect();

            long begin = System.currentTimeMillis();
            long end = begin + timeOut;
            waiting = true;
            while (waiting && System.currentTimeMillis() < end) {
                while (in.available() > 0) {
                    c = (char) in.read();
                    tmpResponse += c;
                    if (tmpResponse.contains("SUCCESS")) {
                        channel.disconnect();
                        waiting = false;
                        break;
                    }
                    // failure in process starting
                    if (tmpResponse.contains("FAIL"))
                        return -1;

                    // no more input
                    if ((int) c < 0)
                        break;
                }
            }
            channel.disconnect();
            session.disconnect();

            if (channel.getExitStatus() == 0) {
                logger.info("Remote Server on host: " + host
                        + " has started! Listening on Port " + arguments[0]);
                return 0;
            } else
                return -1;
        }
        catch (Exception e){
            logger.error(e);
            return -1;
        }
    }

    /**
     * Called to invoke local call without using SSH
     *
     * @param command
     * @param arguments
     * @return
     */
    @Override
    public int invokeProcessLocally(String command, String[] arguments) {
        return 0;
    }
}
