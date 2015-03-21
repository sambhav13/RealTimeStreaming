package goliath.analytics.sparkengine.process;

import goliath.analytics.constants.ApplicationConstants;
import goliath.analytics.errorHandling.GoliathBaseException;
import goliath.analytics.logging.GoliathLogger;
import goliath.analytics.sparkengine.exception.SparkStreamingException;
import goliath.analytics.sparkengine.parsers.FlumeMessageParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;

/**
 * Starts the Spark Streaming code
 * @author sambhav.gupta
 *
 */
@Component("streamReceiver")
public class StreamReceiver {

    private static Properties prop = new Properties();

    private static final Logger log = LoggerFactory.getLogger(StreamReceiver.class);
       
	@Value("${sparkMaster}")
	private String sparkMaster;
	
	
	@Value("${sparkPort}")
	private int sparkPort;
	
	
	@Value("${sparkStreamMaster}")
	private  String sparkStreamMaster;

	@Value("${sparkStreamAppName}")
	private  String sparkStreamAppName;
	
	@Value("${sparkBatchWindow}")
	private int sparkBatchWindow;
	
	@Autowired
	private FlumeMessageParser flumeMessageHandler;
	
	
	/**
     * constructor
     */
	public StreamReceiver()
	{
		
	}
    /**
     * configures 
     */
    public void configureProperties() {

        final String methodName = "configureProperties";
        GoliathLogger.start(log, methodName);
        InputStream inputStream = getClass().getClassLoader().
        		getResourceAsStream(ApplicationConstants.propertyFileName);
        if (inputStream != null) {
            try {
                prop.load(inputStream);
            }
            catch (IOException ioe) {
                GoliathLogger.error(log, methodName, "Not able to find " +
            ApplicationConstants.propertyFileName + "file");
            }
        }
        GoliathLogger.info(log, methodName, "Properties file found and handle configured...");
        GoliathLogger.end(log, methodName);
    }

    /**
     * Creates Java Streaming context for spark streaming
     * @param conf
     * @return Java Streaming context for Spark  Streaming 
     * @throws SparkStreamingException
     */
    private JavaStreamingContext createStreamingContext(SparkConf conf) throws SparkStreamingException {
        final String methodName = "createStreamingContext";
        GoliathLogger.start(log, methodName);
        JavaStreamingContext streamingContext = null;
        try {
            int duration = sparkBatchWindow;
            streamingContext = new JavaStreamingContext(conf, new Duration(duration));
        }
        catch (NumberFormatException nfe) {
            log.error("invalid properties", nfe);
            throw new GoliathBaseException("invalid properties", "createStreamingContext");
        }

        log.info(methodName + ":" + "Spark Streaming context configured...");
        GoliathLogger.info(log, methodName, "Spark Streaming context configured...");
        GoliathLogger.end(log, methodName);
        return streamingContext;
    }

    /**
     * Creates the Spark Conf for Spark Streaming 
     * @return returns the SparkConfiguration
     * @throws SparkStreamingException
     */
    private SparkConf createSparkConf() throws SparkStreamingException {

        final String methodName = "createSparkConf";
        GoliathLogger.start(log, methodName);

        SparkConf sparkConf = new SparkConf();
        //sparkConf.setAppName(prop.getProperty(ApplicationConstants.sparkAppName));
        //sparkConf.setMaster(prop.getProperty(ApplicationConstants.sparkMaster));

        sparkConf.setAppName(sparkStreamAppName);
       sparkConf.setMaster(sparkStreamMaster);
        //sparkConf.setMaster("spark://dev05.goliath.globallogic.com:7077");
        log.info(methodName + ":" + "Spark Conf has been setup...");
        GoliathLogger.info(log, methodName, "Spark Conf has been setup...");
        GoliathLogger.end(log, methodName);
        return sparkConf;
    }

    
    /**
     * Starts the stream process
     * @param streamReceiver
     */
    public void start()
    {
    	 final String methodName = "start";
         GoliathLogger.start(log, methodName);
    	
    	flumeMessageHandler.init();
        flumeMessageHandler.loadRowOder();
        flumeMessageHandler.loadHelper();
        GoliathLogger.info(log, methodName, " Application Context set up..");
        System.out.println("The spark batch window size is"+sparkBatchWindow);
        this.configureProperties();
        String sprkMaster =  sparkMaster;
        int sprkPort = sparkPort;
        try {
            SparkConf sparkConf = this.createSparkConf();
            JavaStreamingContext streamingContext = this.createStreamingContext(sparkConf);
            
            List<JavaDStream<SparkFlumeEvent>> rawFlumeStreamList = new ArrayList<JavaDStream<SparkFlumeEvent>>();
            rawFlumeStreamList.add(FlumeUtils.createStream(streamingContext, sprkMaster, sprkPort));
            log.info(methodName + ":" + "Streaming Started ........");
            GoliathLogger.info(log, methodName, "Streaming Started ........");
            rawFlumeStreamList.get(0);
            JavaDStream<SparkFlumeEvent> rawFlumeStreamFirst = rawFlumeStreamList.get(0);
            rawFlumeStreamList.remove(0);
            JavaDStream<SparkFlumeEvent> unifiedRawFlumeStream = streamingContext.
            		union(rawFlumeStreamFirst, rawFlumeStreamList);
            // Processing parallelism is equal to the number of cores
            JavaDStream<SparkFlumeEvent> rawFlumeStream = unifiedRawFlumeStream.repartition(2);
            System.out.println("Starting stream for flume events..");

            log.info(methodName + ":" + "Starting stream for flume events...");
            GoliathLogger.info(log, methodName, "Starting stream for flume events...");

            rawFlumeStream.foreach(flumeMessageHandler);
            streamingContext.start();
            streamingContext.awaitTermination();
            //((AbstractApplicationContext) context).registerShutdownHook();

        }
        catch (Exception ex) {
            ex.printStackTrace();
            log.error(ex.getMessage(), ex);
            throw new GoliathBaseException(ex.getMessage(), "main");
        }
        GoliathLogger.end(log, methodName);
    }
    
    /**
     * Main Method
     * @param args
     */
    public static void main(String[] args) {
        final String methodName = "main";
        GoliathLogger.start(log, methodName);

        
        ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
        StreamReceiver streamReceiver = (StreamReceiver) context.getBean("streamReceiver");
        streamReceiver.start();
       
        GoliathLogger.end(log, methodName);
        
    }
}
