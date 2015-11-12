package app_kvEcs;


import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ServerInfos implements java.io.Serializable{

    private String ServerName;
    private Integer ServerPort;


    public ServerInfo( String hostname, Integer port ) {
        this.ServerName = hostname;
        this.ServerPort = port;
    }

    public Integer getHostPort() {
        return ServerPort;
    }

    public void setServerPort( Integer HostPort ) {
        this.ServerPort = HostPort;
    }

    public String getServerIP() {
        return ServerName;
    }

    public void setServerIP( String hostname ) {
        ServerName = hostname;
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
