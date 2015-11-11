package app_kvEcs;


import org.apache.log4j.Logger;

import java.util.List;

public class ECS {

    private ConfigReader confReader;
    private List<ServerInfo> Servers;
    private static Logger logger = Logger.getLogger(ECS.class);
    private int CacheSize;
    private String Strategy;


    private boolean addNode(int cacheSize, String displacementStrategy){
        return true;
    }

    private boolean removeNode(){
        return true;
    }

    private boolean start(){
        return true;
    }

    private boolean stop(){
        return true;
    }

    private boolean shutdown(){
        return true;
    }

    private boolean lockWrite(){
        return true;
    }

    private boolean unlockWrite(){
        return true;
    }
}
