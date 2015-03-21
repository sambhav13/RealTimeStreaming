package goliath.analytics.process;

import goliath.analytics.constants.ApplicationConstants;
import goliath.analytics.controller.DomainModelController;
import goliath.analytics.errorHandling.GoliathBaseException;
import goliath.analytics.logging.GoliathLogger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Data Sink for Flume Engine
 * @author sambhav.gupta
 *
 */
public class AnalyticsDataSink extends AbstractSink implements Configurable {
    private EventWriter client = null;


    private DomainModelController domainController;

    private Properties prop;

    private static final Logger log = LoggerFactory.getLogger(AnalyticsDataSink.class);

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
     */
    
    /**
     *  configures the Data Sink
     */
    public void configure(Context context) {

        String methodName = "configure";
        GoliathLogger.start(log, methodName);

        try {
            // Load property file
            prop = new Properties();
            InputStream inputStream = getClass().getClassLoader().
            		getResourceAsStream(ApplicationConstants.propertyFileName);
            if (inputStream != null) {
                prop.load(inputStream);
            }

            String ipAddress = prop.getProperty(ApplicationConstants.flumeInstanceIpAddress);
            int port = Integer.parseInt(prop.getProperty(ApplicationConstants.flumeInstancePort));

            // initialize RPC client for Flume
            client = new EventWriter();
            client.init(ipAddress, port);

            GoliathLogger.info(log, methodName, "The sink is set up with new avro Source...");

            ApplicationContext appContext = new ClassPathXmlApplicationContext("applicationContext.xml");
            domainController = (DomainModelController) appContext.getBean("domainController");

        }
        catch (IOException ioe) {
        	log.error(ioe.getMessage(), ioe);
            throw new GoliathBaseException(ioe.getMessage(), "configure");
        }
        System.out.println(methodName + ":" + "Flume generic Sink configured...");
        log.info(methodName + ":" + "Flume generic Sink configured...");

        GoliathLogger.info(log, methodName, "Flume generic Sink configured...");
        GoliathLogger.end(log, methodName);

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.flume.sink.AbstractSink#start()
     */
    /**
     *  On Start of  the Flume Engine 
     */
    public void start() {
    	domainController.loadHelper();
    	domainController.loadRowOder();
        final String methodName = "start";
        ///System.out.println(methodName + ":" + "Flume Instance started....");
        GoliathLogger.info(log, methodName, "Flume Instance started....");
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.flume.sink.AbstractSink#stop()
     */
    
    /**
     * On Stop of the Flume Engine 
     */
    public void stop() {

        final String methodName = "stop";
        try {
            client.cleanUp();

        }
        catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new GoliathBaseException(ex.getMessage(), "stop");
        }

        ///System.out.println(methodName + ":" + "Flume Instance closed and cleaned ip....");
        GoliathLogger.info(log, methodName, "Flume Instance closed and cleaned ip....");
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.flume.Sink#process()
     */
    
    /**
     *  Flume Events are processed here
     */
    public Status process() throws EventDeliveryException {

        final String methodName = "process";
        GoliathLogger.start(log, methodName);
        Status status = null;
        
        // transaction started
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        txn.begin();
        try {
            Event event = channel.take();
           
            if (event != null) {
                byte[] eventBody = event.getBody();

                System.out.println("Event received is " + eventBody);
                GoliathLogger.info(log, methodName, "Event received is " + eventBody);
                String domainObservation = domainController.
                		getProcessedData(eventBody, ApplicationConstants.FlumeEngine);
                System.out.println("Read Response by hdfs sink : " + domainObservation);

                client.forwardParsedDataToFlumeInstance(domainObservation);

                log.info(methodName + ":" + "Domain observation is pushed to HDFS for persistence");
               System.out.println("Domain observation is pushed to HDFS for persistence");
            }
            txn.commit();
            status = Status.READY;

            GoliathLogger.info(log, methodName, "Flume Event Recieved....");
            GoliathLogger.end(log, methodName);

        }
        catch (Throwable t) {
            txn.rollback();
            // Log exception, handle individual exceptions as needed
            status = Status.BACKOFF;
            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error) t;
            }
        }
        finally {
            txn.close();
        }
        return status;
    }
}