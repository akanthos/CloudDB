package app_kvEcs;

import org.apache.log4j.Logger;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.File;
import java.io.InputStream;

/**
 * class handling the start of a server process
 * locally or remotely
 */
public class CallRemote implements CallRemoteInterface {

    private String privateKey = System.getProperty("user.home") + "/.ssh/id_rsa";
    private String knownHosts = System.getProperty("user.home") + "/.ssh/known_hosts";
    public static Logger logger = Logger.getRootLogger();
    private String userName = System.getProperty("user.name");
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
    public int RunRemoteProcess(String host, String command, String[] arguments) {

        boolean waiting;
        String tmpResponse = "";
        char c;
        command = "nohup java -jar " + "/home/pi/clouddb" + "/ms5-server.jar ";

        try {
            JSch jsch = new JSch();
            jsch.setKnownHosts(knownHosts);
            jsch.addIdentity(privateKey);
            Session session = jsch.getSession("pi", host, port);
            session.setPassword("raspberry");
            session.setConfig("StrictHostKeyChecking", "no"); //
            session.setConfig("PreferredAuthentications", "password,gssapi-with-mic,publickey");
            session.connect(3000);
            // Add arguments to exec Command
            for (String argument : arguments)
                command += " " + argument;

            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            channel.setInputStream(null);
            InputStream in = channel.getInputStream();
            channel.connect();

            waiting = true;
            while (waiting) {
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
    public int RunLocalProcess(String command, String[] arguments) {

        //ERROR & -> dont write standard error in nohup.out
        try {
            // adding the arguments to the command
            for (String argument : arguments)
                command += " " + argument;
            ProcessBuilder processb = new ProcessBuilder("nohup", "java", "-jar", "ms5-server.jar",
                    arguments[0], arguments[1], arguments[2], "&");
            String path = System.getProperty("user.dir");

            processb.directory(new File(path));
            Process p = processb.start();
            InputStream in = p.getInputStream();
            long begin = System.currentTimeMillis();
            long end = begin + timeOut;
            String s = "";
            char c;
            boolean waiting = true;
            while (waiting) {
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
            logger.info("Local Server on port " + arguments[0]);
            return 0;
        } catch (Exception e) {
            System.out.println(e);
            logger.error(e);
            return -1;
        }

    }

}
