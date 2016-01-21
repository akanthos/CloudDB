package app_kvServer;

import java.util.HashSet;
import java.util.Set;

/**
 * This class represents a user subscription. It is associated
 * to a specific key.
 */
public class ClientSubscription {

    private String address;
    private int port;
    private Set<Interest> interests;


    public enum Interest { CHANGE, DELETE, CHANGE_DELETE }

    /**
     * Constructor
     * @param inetAddress the IP address of the user to be notified
     * @param interest the subscription type of the user for the specific key
     *                 the object is associated to.
     */
    public ClientSubscription(String inetAddress, int port, Interest interest) {
        this.address = inetAddress;
        this.port = port;
        this.interests = new HashSet<>();
        if (interest == Interest.CHANGE_DELETE) {
            this.interests.add(Interest.CHANGE);
            this.interests.add(Interest.DELETE);
        }
        else {
            this.interests.add(interest);
        }
    }

    /**
     * Checks if the user subscription includes a specific type of action.
     * @param interest the action to be checked if it is included
     *                 in the subscription
     * @return true if it is included, false if not.
     */
    public boolean isInterestedIn(Interest interest) {
        return interests.contains(interest);
    }

    /**
     * Adds an action interest to the current subscription
     * @param interest the action that needs to be added to the subscription
     */
    public void addInterest(Interest interest) {
        this.interests.add(interest);
    }

    /**
     * Remove an action interest from the current subscription
     * @param interest
     */
    public void removeInterest(Interest interest) {
        this.interests.remove(interest);
    }

    /**
     * Gets all the action interests of the current subscription
     * @return the current interests
     */
    public Set<Interest> getInterests() {
        return interests;
    }

    /**
     * User's IP address getter
     * @return the user's IP address
     */
    public String getAddress() {
        return address;
    }

    /**
     * Ordinal representation of the action interests.
     * @return the ordinal number that corresponds to the current
     *          actions that the subscription includes.
     */
    public Integer getInterestsOrdinal() {
        if (isInterestedIn(ClientSubscription.Interest.CHANGE) && isInterestedIn(ClientSubscription.Interest.DELETE)) {
            return ClientSubscription.Interest.CHANGE_DELETE.ordinal();
        } else if (isInterestedIn(ClientSubscription.Interest.CHANGE)) {
            return ClientSubscription.Interest.CHANGE.ordinal();
        } else if (isInterestedIn(ClientSubscription.Interest.DELETE)) {
            return ClientSubscription.Interest.DELETE.ordinal();
        }
        return -1;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

}
