package common;


import common.messages.KVMessageImpl;
import common.utils.KVRange;
import hashing.MD5Hash;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ServerInfo implements java.io.Serializable{

    private String ServerName;
    private Integer ServerPort;
    private String address;
    private KVRange ServerRange;

    private static Logger logger = Logger.getLogger(KVMessageImpl.class);

    static {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
    }

    public ServerInfo(String messageString) throws Exception {
        try {
            String[] msgParts = messageString.split(",");
            address = msgParts[0];
            ServerPort = Integer.parseInt(msgParts[1]);
        } catch (Exception e) {
            logger.error(String.format("Unable to construct ServerInfo from message: %s", messageString), e);
            throw new Exception("Unknown message format");
        }
    }


    public ServerInfo(String address, Integer port) {
        this.address = address;
        this.ServerPort = port;
    }

    public ServerInfo(String address, Integer port, KVRange range) {
        this.address = address;
        this.ServerPort = port;
        this.ServerRange = range;
    }

    public Integer getServerPort() {
        return ServerPort;
    }

    public void setServerPort( Integer serverPort ) {
        this.ServerPort = serverPort;
    }

    @Override
    public String toString() {
        return this.getAddress()+","+this.getServerPort();
    }

    public long getHash() {
        MD5Hash md5 = new MD5Hash();
        return md5.hash(this.toString());
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }


    public KVRange getServerRange() {
        return ServerRange;
    }

    public void setServerRange(KVRange serverRange) {
        ServerRange = serverRange;
    }

}
