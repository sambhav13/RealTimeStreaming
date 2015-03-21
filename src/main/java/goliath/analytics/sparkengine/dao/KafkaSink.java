package goliath.analytics.sparkengine.dao;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import kafka.common.KafkaException;
import kafka.producer.KeyedMessage;
import goliath.analytics.constants.ApplicationConstants;
import goliath.analytics.errorHandling.GoliathBaseException;
import goliath.analytics.logging.GoliathLogger;
import goliath.analytics.sparkengine.util.ProducerRegistrator;

/**
 * Kafka Sink to Handle data persistence to Kafka Topics
 * @author sambhav.gupta
 *
 */
@Component("kf")
public class KafkaSink {

	private kafka.javaapi.producer.Producer<String, String> producer = null;

    private Properties kafkaProperties = null;

    private Properties prop = null;

    private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);
	
	@Autowired
    private ProducerRegistrator pR ;

    
    
	@Value("${kafkaBrokerList}")
	private String kafkaBrokerList;
	
    
    @Value("${kafkaEncoder}")
	private String kafkaEncoder;
        

	/**
	 *  Constructor
	 */
	public KafkaSink() {
   
        final String methodName = "KafkaSink";
        GoliathLogger.start(log, methodName);

        System.out.println("Kafka Sink initialized");

        log.info(methodName + ":" + "KafkaFlusher  handles configured...");
        GoliathLogger.info(log, methodName, "KafkaFlusher  handles configured...");
        GoliathLogger.end(log, methodName);
    }
    

	/**
	 *  Initialises the Kafka Sink
	 */
	public void init(){
	  
	  kafkaProperties = new Properties();
      kafkaProperties.put(ApplicationConstants.metaBrokerList_PropName, kafkaBrokerList);
      kafkaProperties.put(ApplicationConstants.serializerClass_PropName, kafkaEncoder);
      /*kafkaProperties.put(ApplicationConstants.serializerClass_PropName, 
      prop.getProperty(ApplicationConstants.kafka_Encoder));*/
      
      pR.registerProducers(kafkaProperties);
      pR.mapProducers();
  }
    /**
     * Inserts Row to kafka Topic using respective Producer
     * @param row
     * @param topic
     * @param domainId
     * inserts row in the specific topic for a given domain
     */
    public void insertRow(String row, String topic, String domainId) {
        final String methodName = "insertRow";
        GoliathLogger.start(log, methodName);

        producer = getProducer(domainId);
        KeyedMessage<String, String> message = new KeyedMessage<String, String>(topic, row);
        try {
            if (producer != null) {
                System.out.println("producer not null ");
                GoliathLogger.info(log, methodName, "producer not null ");
            }
            producer.send(message);
        }
        catch (KafkaException ke) {
            log.error("Kafka Exception", ke);
            throw new GoliathBaseException(ke.getMessage(), "insertRow");
        }

        log.info(methodName + ":" + "Kafka row Flushed...");
        GoliathLogger.info(log, methodName, "Kafka row Flushed...");
        GoliathLogger.end(log, methodName);
    }

    /**
     * Gets the the Respective domain Producer
     * @param domainId
     * @return the domain specific producer
     */
    public kafka.javaapi.producer.Producer<String, String> getProducer(String domainId) {
        final String methodName = "getProducer";
        GoliathLogger.start(log, methodName);

        log.info(methodName + ":" + "Kafka Producer Returned...");
        GoliathLogger.info(log, methodName, "Kafka Producer Returned...");
        GoliathLogger.end(log, methodName);
        return pR.getProducer(domainId);
    }

    /**
     *  Closes the kafka Producers
     */
    public void closeKafkaFluser() {
       
    	final String methodName = "getProducer";
        GoliathLogger.start(log, methodName);

        pR.closeProducers();

        log.info(methodName + ":" + "Kafka Producers Handles Closed...");
        GoliathLogger.info(log, methodName, "Kafka Producers Handles Closed...");
        GoliathLogger.end(log, methodName);
    }

}
