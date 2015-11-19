package common;


import common.utils.KVRange;

import common.messages.KVMessageImpl;

import hashing.MD5Hash;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class ServerInfo implements java.io.Serializable, Comparable<ServerInfo> {

    private String ServerName;
    private Integer serverPort;
    private String address;
    private KVRange serverRange;
    private boolean isLaunched;


    private static Logger logger = Logger.getLogger(KVMessageImpl.class);

    static {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
    }

    public ServerInfo(String messageString) throws Exception {
        try {
            String[] msgParts = messageString.split(",");
            address = msgParts[0];
            serverPort = Integer.parseInt(msgParts[1]);
        } catch (Exception e) {
            logger.error(String.format("Unable to construct ServerInfo from message: %s", messageString), e);
            throw new Exception("Unknown message format");
        }
    }


    public ServerInfo(String address, Integer port) {
        this.address = address;
        this.serverPort = port;
        this.serverRange = new KVRange();
    }

    public ServerInfo(String address, Integer port, KVRange range) {
        this.address = address;
        this.serverPort = port;
        this.serverRange = range;
    }

    public Integer getServerPort() {
        return serverPort;
    }

    public void setServerPort( Integer serverPort ) {
        this.serverPort = serverPort;
    }

    @Override
    public String toString() {
        return this.getAddress()+","+this.getServerPort();
    }

    public Long getHash() {
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
        return serverRange;
    }

    public void setServerRange(KVRange serverRange) {
        this.serverRange = serverRange;
    }

    public Long getFromIndex() {
        return serverRange.getLow();
    }

    public void setFromIndex(Long fromIndex) {
        this.serverRange.setLow(fromIndex);    }

    public Long getToIndex() {
        return serverRange.getHigh();
    }

    public void setToIndex(Long toIndex) {
        this.serverRange.setHigh(toIndex);
    }

    public void setLaunched(boolean isLaunched) { this.isLaunched = isLaunched; }

    @Override
    public int compareTo(ServerInfo o) {
        Long l = serverRange.getLow();
        return l.compareTo(o.getServerRange().getLow());
    }
}
