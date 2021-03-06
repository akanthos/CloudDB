package app_kvEcs;

import org.apache.log4j.Logger;

/**
 * main ECS Application
 */
public class ECSApp {

    private Logger logger = Logger.getLogger( this.getClass ());
    private ECSClient ecsClient;

    public ECSApp(String fileName) {
        this.ecsClient = new ECSClient(fileName);
        this.ecsClient.run();

    }

    public static void main(String[] args) {
        if (args.length == 1){
            new ECSApp(args[0]);
        }
        else{
            System.out.println("Wrong arguments given. Run app using ECSInterface " +
                    "Configuration file with store servers information as the only parameter.");
        }
    }

}
