package client;

import common.messages.KVMessage;

public interface KVCommInterface {

	/**
	 * Establishes a connection to the KV KVServer.
	 * 
	 * @throws Exception
	 *             if connection could not be established.
	 */
	public void connect(String hostAddress, Integer port) throws Exception;

	/**
	 * disconnects the client from the currently connected server.
	 */
	public void disconnect(boolean full);

	/**
	 * Inserts a key-value pair into the KVServer.
	 * 
	 * @param key
	 *            the key that identifies the given value.
	 * @param value
	 *            the value that is indexed by the given key.
	 * @return a message that confirms the insertion of the tuple or an error.
	 * @throws Exception
	 *             if put command cannot be executed (e.g. not connected to any
	 *             KV server).
	 */
	public KVMessage put(String key, String value) throws Exception;

	/**
	 * Retrieves the value for a given key from the KVServer.
	 * 
	 * @param key
	 *            the key that identifies the value.
	 * @return the value, which is indexed by the given key.
	 * @throws Exception
	 *             if put command cannot be executed (e.g. not connected to any
	 *             KV server).
	 */
	public KVMessage get(String key) throws Exception;

	/**
	 * Subscribes to a key with the corresponsing coordinator node
	 *
	 * @param key
	 * @return
	 * @throws Exception
     */
	public KVMessage subscribe(String key);

	/**
	 * unsubscribes to a key with the corresponsing coordinator node
	 *
	 * @param key
	 * @return
	 * @throws Exception
	 */
	public KVMessage unsubscribe(String key);
}
