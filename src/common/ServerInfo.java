package common;


import common.utils.KVRange;

import common.messages.KVMessageImpl;

import hashing.MD5Hash;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Class representing the basic information about a server instance
 */
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

    /**
     * Default constructor
     */
    public ServerInfo(){}

    /**
     * Constructor
     *
     * @param address server address
     * @param port server port
     */
    public ServerInfo(String address, Integer port) {
        this.address = address;
        this.serverPort = port;
        this.serverRange = new KVRange();
    }

    /**
     * Constructor
     *
     * @param address server address
     * @param port server port
     * @param range server key range
     */
    public ServerInfo(String address, Integer port, KVRange range) {
        this.address = address;
        this.serverPort = port;
        this.serverRange = range;
    }

    /**
     * Server ID string getter
     * @return a string of the form "IP:port"
     */
    public String getID() {
        return address + ":" + serverPort;
    }

    /**
     * Port getter
     * @return server port
     */
    public Integer getServerPort() {
        return serverPort;
    }

    /**
     * Port setter
     * @param serverPort
     */
    public void setServerPort( Integer serverPort ) {
        this.serverPort = serverPort;
    }

    @Override
    public String toString() {
        return this.getAddress()+","+this.getServerPort();
    }

    /**
     * Computes sever hash position on the ring
     *
     * @return
     */
    public String getHash() {
        MD5Hash md5 = new MD5Hash();
        return md5.hash(this.toString());
    }

    /**
     * Server address getter
     * @return
     */
    public String getAddress() {
        return address;
    }

    /**
     * Server address setter
     * @param address
     */
    public void setAddress(String address) {
        this.address = address;
    }

    /**
     * Server key range getter
     * @return
     */
    public KVRange getServerRange() {
        return serverRange;
    }

    /**
     * Server key range setter
     * @param serverRange
     */
    public void setServerRange(KVRange serverRange) {
        this.serverRange = serverRange;
    }

    /**
     * Low limit of the server instance key range getter
     * @return
     */
    public String getFromIndex() {
        return serverRange.getLow();
    }

    /**
     * Low limit of the server instance key range setter
     * @param fromIndex
     */
    public void setFromIndex(String fromIndex) {
        this.serverRange.setLow(fromIndex);    }

    /**
     * High limit of the server instance key range getter
     * @return
     */
    public String getToIndex() {
        return serverRange.getHigh();
    }

    /**
     * High limit of the server instance key range setter
     * @param toIndex
     */
    public void setToIndex(String toIndex) {
        this.serverRange.setHigh(toIndex);
    }

    /**
     * Launched state setter
     * @param isLaunched
     */
    public void setLaunched(boolean isLaunched) { this.isLaunched = isLaunched; }

    /**
     * Server position on the ring comparator
     * @param o
     * @return
     */
    @Override
    public int compareTo(ServerInfo o) {
        String l = serverRange.getLow();
        return l.compareTo(o.getServerRange().getLow());
    }
}
