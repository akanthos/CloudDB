package app_kvServer;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by akanthos on 15.01.16.
 */
public class ClientSubscription {
    private String address;
    private Set<Interest> interests;


    public enum Interest { CHANGE, DELETE, CHANGE_DELETE }

    public ClientSubscription(String inetAddress, Interest interest) {
        this.address = inetAddress;
        this.interests = new HashSet<>();
        if (interest == Interest.CHANGE_DELETE) {
            this.interests.add(Interest.CHANGE);
            this.interests.add(Interest.DELETE);
        }
        else {
            this.interests.add(interest);
        }
    }

    public boolean isInterestedIn(Interest interest) {
        return interests.contains(interest);
    }

    public void addInterest(Interest interest) {
        this.interests.add(interest);
    }

    public void removeInterest(Interest interest) {
        this.interests.remove(interest);
    }

    public Set<Interest> getInterests() {
        return interests;
    }

    public String getAddress() {
        return address;
    }

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
}
