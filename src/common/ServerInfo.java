package common;


import common.utils.KVRange;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ServerInfo implements java.io.Serializable{

    private String ServerName;
    private Integer ServerPort;
    private String address;
    private KVRange ServerRange;


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
        return this.getAddress()+":"+this.getServerPort();
    }

    public String getHash() throws UnsupportedEncodingException, NoSuchAlgorithmException {
        byte[] bytesOfMessage = this.toString().getBytes("UTF-8");
        MessageDigest mdEnc = MessageDigest.getInstance("MD5");
        mdEnc.update(bytesOfMessage, 0, bytesOfMessage.length);
        String md5 = new BigInteger(1, mdEnc.digest()).toString(16); // Hash value
        return md5;
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
