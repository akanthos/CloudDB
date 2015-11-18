package app_kvEcs;

import java.util.List;
import common.ServerInfo;
import common.messages.AbstractMessage;

/**
 *
 * The Class of ECS Commands.
 * Sent to servers.
 */
public class ECSMsg implements AbstractMessage {

    private ECSCommand actionType;
    private List<ServerInfo> metaData;
    private String FromIndex;
    private String ToIndex;
    private ServerInfo ToServer;


    @Override
    public MessageType getMessageType() {
        return MessageType.ECS_MESSAGE;
    }

    public ECSCommand getActionType() {
        return actionType;
    }

    public void setActionType(ECSCommand actionType) {
        this.actionType = actionType;
    }

    public List<ServerInfo> getMetaData() {
        return metaData;
    }

    public void setMetaData(List<ServerInfo> metaData) {
        this.metaData = metaData;
    }

    public ServerInfo getToServer() {
        return ToServer;
    }

    public void setToServer(ServerInfo moveToServer) {
        this.ToServer = moveToServer;
    }

    public String getFromIndex() {
        return FromIndex;
    }

    public void setFromIndex(String moveFromIndex) {
        this.FromIndex = moveFromIndex;
    }

    public String getToIndex() {
        return ToIndex;
    }

    public void setToIndex(String moveToIndex) {
        this.ToIndex = moveToIndex;
    }

}