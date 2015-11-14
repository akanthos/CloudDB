package app_kvEcs;


import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ServerInfos implements java.io.Serializable{

    private String host;
    private Integer port;


    public ServerInfos( String hostname, Integer port ) {
        this.host = hostname;
        this.port = port;
    }

    public Integer getHostPort() {
        return port;
    }

    public void setPort(Integer HostPort ) {
        this.port = HostPort;
    }

    public String getServerIP() {
        return host;
    }

    public void setServerIP( String hostname ) {
        host = hostname;
    }


    @Override
    public String toString() {
        return this.getServerIP()+":"+this.getServerIP();
    }

    public String getHash() throws UnsupportedEncodingException, NoSuchAlgorithmException {
        byte[] bytesOfMessage = this.toString().getBytes("UTF-8");
        MessageDigest mdEnc = MessageDigest.getInstance("MD5");
        mdEnc.update(bytesOfMessage, 0, bytesOfMessage.length);
        String md5 = new BigInteger(1, mdEnc.digest()).toString(16); // Hash value
        return md5;
    }

}
