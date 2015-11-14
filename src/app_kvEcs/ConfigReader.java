package app_kvEcs;

import common.ServerInfo;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfigReader {

    private String fileName;
    private final String NODE_PATTERN = "[A-Za-z0-9]+";
    private final String HOSTNAME_PATTERN = "(((?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.)" +
                                            "{3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))|localhost)";
    private final String PORT_PATTERN = "(\\d+)";
    private final Pattern LINE_PATTERN = Pattern.compile( "(?m)^" + NODE_PATTERN + "(\\s)+" + HOSTNAME_PATTERN
                                                            + "(\\s)+" + PORT_PATTERN + "$");

    private List<ServerInfo> servers = new ArrayList<ServerInfo>();

    /**
     * Constructor
     * @param fileName name of the configuration File
     */
    public ConfigReader( String fileName ) throws IOException {

        BufferedReader reader = null;
        try {
            reader = new BufferedReader( new FileReader( fileName ));
            for ( String line; null != (line = reader.readLine()); ) {
                parseLine( line );
            }

        } catch ( IOException e ) {
            throw new IOException( "Cannot access file: " + e );
        }
        finally {
            if ( null != reader ) try {
                reader.close();
            } catch ( IOException e ) {
                System.err.println( "Could not close " + fileName + " - " + e );
            }
        }
    }

    /**
     * Reads a line from config file and adds a ServerInfo
     * element to the servers List Collection.
     * @param line line reading from file
     */
    public void parseLine( final String line ) {

        final Matcher matcher = LINE_PATTERN.matcher(line);
        if ( ! matcher.matches() ) {
            System.err.println( "Bad config line: " + line );
            return;
        }
        String[] splited = line.split("\\s+");
        servers.add( new ServerInfo( splited[1], Integer.parseInt(splited[2])));

    }

    /**
     *
     * @return List of ServerInfo objects
     */
    public List<ServerInfo> getServers(){
        return servers;
    }

}
