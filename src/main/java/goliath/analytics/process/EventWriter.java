package goliath.analytics.process;

import goliath.analytics.logging.GoliathLogger;

import java.nio.charset.Charset;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes the Event To another Flume Instance
 * @author sambhav.gupta
 *
 */
public class EventWriter {

    private RpcClient client;

    private String hostname;

    private int port;

    private static final Logger log = LoggerFactory.getLogger(EventWriter.class);

    /**
     * 
     * This method created RPC flume connection.
     * @param hostname
     * @param port
     */
    public void init(String hostname, int port) {
        String methodName = "init";

        this.hostname = hostname;
        this.port = port;
        this.client = RpcClientFactory.getDefaultInstance(hostname, port);

        /*///System.out.println(methodName + ":" + 
        "Flume RPC Client initialised on hostName" + hostname + " and port " + port);*/
        GoliathLogger.info(log, methodName, 
        		"Flume RPC Client initialised on hostName" + hostname + " and port " + port);
    }

    
    
    /**
     * This method forwards processed observation to another flume instance to
     * persist on HDFS.
     * 
     * @param observation : Parsed observation to forward to Flume instance.
     */
    public void forwardParsedDataToFlumeInstance(String observation) {
        String methodName = "forwardParsedDataToFlumeInstance";
        GoliathLogger.start(log, methodName);
        Event event = EventBuilder.withBody(observation, Charset.forName("UTF-8"));
   
        try {
            client.append(event);
        }
        catch (EventDeliveryException e) {
           
            client.close();
            client = null;
            client = RpcClientFactory.getDefaultInstance(hostname, port);
        }

        log.info(methodName + ":" + "RPC Flume Client:Data to Flume Instance sent...!!");
        GoliathLogger.info(log, methodName, "RPC Flume Client:Data to Flume Instance sent...!!");
        GoliathLogger.end(log, methodName);
    }
    
    

    /**
     * Closes the RPC client (connection).
     */
    public void cleanUp() {
        String methodName = "cleanUp";
       
        client.close();
        System.out.println(methodName + ":" + "RPC Flume Client closed and cleaned up...!!");
        GoliathLogger.info(log, methodName, "RPC Flume Client closed and cleaned up...!!");

    }
}
