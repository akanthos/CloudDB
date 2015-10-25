package app_kvServer;


public class KVServerNew {

	private KVCache dataCache = null;

	private static final int KEY_SIZE = 20;
	private static final int VAL_SIZE = 120 * 1024;


	public KVServerNew(int cacheSize, int other, String Policy) {

		dataCache = new KVCache(cacheSize, Policy);

	}

	//put to cache
	public void put(String key, String value) throws KVException {
		// Must be called before anything else
	}

	//get from cache
	public KVConnectionMessage get (String key) throws KVException {
		// Must be called before anything else
		KVConnectionMessage result = new KVConnectionMessage("hello");
		return result;
	}

	//return cache
	public KVCache getCache() {
		return dataCache;
	}


}