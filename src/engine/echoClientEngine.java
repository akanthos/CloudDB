package engine;

public class echoClientEngine {
	private String hostIp;
	private String hostPort;
	private boolean isConnected;

	public echoClientEngine(String hostIp, String hostPort) {
		this.hostIp = hostIp;
		this.hostPort = hostPort;
//		Try to connect...
	}
	
	public boolean getIsConnected(){
		return this.isConnected;
	}
	
}
