package common.messages;

import hashing.MD5Hash;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

/**
 * Client message interface
 */
public interface KVMessage extends AbstractMessage {
	
	enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request successful */
		GENERAL_ERROR, 	/* If an unexpected situation is encountered */
		SERVER_STOPPED,         /* Server is stopped, no requests are processed */
		SERVER_WRITE_LOCK,      /* Server locked for out, only get possible */
		SERVER_NOT_RESPONSIBLE,  /* Request not successful, server not responsible for key */
		SUBSCRIBE_CHANGE,
		SUBSCRIBE_DELETE,
		SUBSCRIBE_CHANGE_DELETE,
		SUBSCRIBE_SUCCESS,
		SUBSCRIBE_ERROR,
		UNSUBSCRIBE_ERROR,
		UNSUBSCRIBE_CHANGE,
		UNSUBSCRIBE_DELETE,
		UNSUBSCRIBE_CHANGE_DELETE,
		UNSUBSCRIBE_SUCCESS,
		NOTIFICATION_KEY_CHANGED,
		NOTIFICATION_KEY_DELETED
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	StatusType getStatus();

	/**
	 * Status setter
	 * @param statusType
     */
	void setStatus(StatusType statusType);

	/**
	 * Computes the hash value of the message
	 * @return
     */
	String getHash();

	public String getAddress();
	public void setAddress(String address);


	public Integer getPort();

	public void setPort(Integer port);
	
}


