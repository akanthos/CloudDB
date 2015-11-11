package app_kvEcs;


public class ServerInfo implements java.io.Serializable{

    private String ServerName;
    private String ServerPort;


    public ServerInfo( String hostname, String port ) {

        this.ServerName = hostname;
        this.ServerPort = port;
    }

    public String getHostPort() {
        return ServerPort;
    }

    public void setServerPort( String HostPort ) {
        this.ServerPort = HostPort;
    }

    public String getServerIP() {
        return ServerName;
    }

    public void setServerIP( String hostname ) {
        ServerName = hostname;
    }


    @Override
    public String toString() {
        return this.getServerIP()+":"+this.getServerIP();
    }

}
