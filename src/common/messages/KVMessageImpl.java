package common.messages;

import common.Serializer;
import common.ServerInfo;
import common.utils.Utilities;
import hashing.MD5Hash;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * KVMessage represantion of connection Message
 */
public class KVMessageImpl implements KVMessage {

    String key;
    String value;
    StatusType status;
    String address = "localhost";
    Integer port = 0;


    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }


    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }
    /* represents meta-data of all nodes in the system */
    private List< ServerInfo > metadata;

    private static Logger logger = Logger.getLogger(KVMessageImpl.class);

    static {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
    }

    public KVMessageImpl(String[] tokens) {
        if (tokens[1] != null) {// status
            int statusNum = Integer.parseInt(tokens[1]);
            this.setStatus( KVMessage.StatusType.values()[statusNum] );
        }
        if (tokens[2] != null) { // key
            this.setKey(tokens[2]);
        }
        if (tokens.length >= 4) {
            if (tokens[3] != null) { // value
                this.setValue(tokens[3].trim());
            }
            if (tokens[4] != null) { // address
                this.setAddress(tokens[4].trim());
            }
            if (tokens[5] != null) { // port
                this.setPort(Integer.parseInt(tokens[5].trim()));
            }
        }
        if (tokens.length > 6) {List<ServerInfo> metaData = Serializer.getMetaData(tokens[6].trim());
            this.setMetadata(metaData);
        }
    }

    public KVMessageImpl (StatusType status) {
        this.status = status;
    }

    /**
     * Constructor
     * @param key Key of the connection KV Message
     * @param value Value of the connection KV Message
     * @param status Status of the connection KV Message
     */
    public KVMessageImpl(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public KVMessageImpl(String key, String value, StatusType status, String address, int port) {
        this.key = key;
        this.value = value;
        this.status = status;
        this.address = address;
        this.port = port;
    }

    /**
     * Constructor
     * @param metadata Key of the connection KV Message
     * @param status Status of the connection KV Message
     */
    public KVMessageImpl(List<ServerInfo> metadata, StatusType status) {
        this.status = status;
        this.setMetadata(metadata);
    }

    public KVMessageImpl(String key,  String value, List<ServerInfo> metadata, StatusType status) {
        this.status = status;
        this.key = key;
        this.value = value;
        this.setMetadata(metadata);
    }

    /**
     * Key getter
     * @return key of Message
     */
    @Override
    public String getKey() {
        return key;
    }

    /**
     * Value getter
     * @return Value of Message
     */
    @Override
    public String getValue() {
        return value;
    }

    /**
     * Status getter
     * @return Status of Message
     */
    @Override
    public StatusType getStatus() {
        return status;
    }

    /**
     * Key setter
     * @param key key to set in Message
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * Key setter
     * @param value value to set in Message
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     * Status setter
     * @param status status to set in Message
     */
    @Override
    public void setStatus(StatusType status) {
        this.status = status;
    }

    /**
     * toString method for KV Message
     * @return String representation of Message
     */
    public String toString() {
        // TODO: This is obsolete, change it using serializer directly, not from bytes
        return getMsgBytes().toString();
    }

    /**
     *
     * @return bytes repreentation of Message
     */
    public byte[] getMsgBytes() {
        return Serializer.toByteArray(this);
    }

    /**
     * Computes the hash value of the message
     * @return
     */
    public String getHash() {
        MD5Hash md5 = new MD5Hash();
        return md5.hash(key);
    }

    /**
     * Metadata getter
     * @return the metadata that are part of the message
     */
    public List<ServerInfo> getMetadata() {
        return metadata;
    }

    /**
     * Metadata setter
     * @param metadata the metadata to be set
     */
    public void setMetadata(List<ServerInfo> metadata) {
        this.metadata = metadata;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.CLIENT_MESSAGE;
    }
}
