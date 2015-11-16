package hashing;


import java.util.*;

public class Ring<T> {

    private SortedMap<Long, T> smap = new TreeMap<Long, T>();
    private MD5Hash hash = new MD5Hash();

    public Ring(List<T> pNodes) {
        for (T pNode : pNodes) {
            addVnode(pNode);
        }
    }

    /**
     * Add node to Ring topology
     * pNode is most likely of type <ServerInfo>
     * @param pNode
     */
    public void addVnode (T pNode){
        smap.put( hash.hash(pNode.toString()), pNode );
    }

    /**
     * Remove node from Ring topology
     * pNode is most likely of type <ServerInfo>
     * @param pNode
     */
    public void removeVnode (T pNode) {
        smap.remove( pNode.toString() );
    }

    /**
     * Get object from object ID
     * @param objectId
     * @return
     */
    private T getNodeByObjectId(String objectId) {

        Long hashValue = hash.hash(objectId);
        if (!smap.containsKey(hashValue)) {
            SortedMap<Long, T> tailMap = smap.tailMap(hashValue);
            hashValue = tailMap.isEmpty() ? smap.firstKey() : tailMap.firstKey();
        }
        return smap.get(hashValue);
    }

    /**
     * Metadata: Sorted Map <Node_ID> < ServerInfo> : Node_ID is the hash("IP"+"port")
     * @return Sorted Map respresenting Ring
     */
    public SortedMap<Long, T> getMetaData(){
        return this.smap;
    }
}
