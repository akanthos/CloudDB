package app_kvServer;

import common.utils.KVMetadata;

/**
 * Created by akanthos on 11.11.15.
 */
public interface ServerActionListener {
    void updateState(ServerState s);

    void updateMetadata(KVMetadata metadata);
}
