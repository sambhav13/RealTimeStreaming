package goliath.analytics.sparkengine.parsers;

import goliath.analytics.controller.DomainModelController;
import goliath.analytics.sparkengine.dao.KafkaSink;
import goliath.analytics.constants.ApplicationConstants;
import goliath.analytics.logging.GoliathLogger;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import java.util.StringTokenizer;

import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Flume main controller class to handle continuos flume events
 * @author sambhav.gupta
 *
 */
@Component("flumeMessageHandler")
public class FlumeMessageParser  implements Function<JavaRDD<SparkFlumeEvent>, Void>{

	private static final long serialVersionUID = 8117786034856910640L;
	
	private static final Logger log = LoggerFactory.getLogger(FlumeMessageParser.class);	

	@Autowired
	private DomainModelController domainController;

	@Autowired
	private KafkaSink kf;


	/**
	 *  Initializes the respective handles
	 */
	
	/*public DomainModelController getDomainModelController( ){
	      return domainController;
	   }
	
	public KafkaSink getKafkaSink( ){
	      return kf;
	   }
	
	public void setDomainModelController(DomainModelController domainController) {
        this.domainController = domainController;
    }
	
	public void setKafkaSink(KafkaSink kf) {
        this.kf = kf;
    }*/

	/**
	 *  intialises the Flume Message Parser
	 */
	public void init() {

		final String methodName = "init";
		GoliathLogger.start(log, methodName);

		//ApplicationContext appContext = new ClassPathXmlApplicationContext("applicationContext.xml");
		//domainController = (DomainModelController) appContext.getBean("domainModelController");
		//kf =  (KafkaSink)appContext.getBean("kafkaSink");
		kf.init();
		log.info(methodName+":"+"FlumeMessage Pasrser initialized...");
		GoliathLogger.info(log, methodName, "FlumeMessage Pasrser initialized...");
		GoliathLogger.end(log, methodName);
	}

	/**
	 * loads the row oder
	 */
	public void loadRowOder()
	{
		domainController.loadRowOder();
	}
	
	/**
	 *  loads the helper handles
	 */
	public void loadHelper() {
		domainController.loadHelper();
	
	}



	/* Continuosly receives events from the flume stream
	 *  (non-Javadoc) 
	 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
	 */
	
	
	/** 
	 * call method of Java Streaming for spark
	 */
	public Void call(JavaRDD<SparkFlumeEvent> eventsData) throws Exception {

		final String methodName = "call";
		GoliathLogger.start(log, methodName);
		List<SparkFlumeEvent> events = eventsData.collect();
		if(events.size()!=0) 
		{
			Iterator<SparkFlumeEvent> batchedEvents = events.iterator();
			AvroFlumeEvent avroEvent = null;
			ByteBuffer bytePayload = null;
			
			while(batchedEvents.hasNext()) 
			{
				SparkFlumeEvent flumeEvent = batchedEvents.next();
				avroEvent = flumeEvent.event();
				bytePayload = avroEvent.getBody();				
				byte[] flumeData = bytePayload.array();
				log.info(methodName+":"+"Event Received...");
				GoliathLogger.info(log, methodName, "Event Received...");

				String row = domainController.getProcessedData(flumeData,
						ApplicationConstants.SparkEngine);				
				StringTokenizer st = new StringTokenizer(row, "##");			

				System.out.println("The returnde row is--->"+row);
				log.info(methodName+":"+"The returnde row is--->"+row);
				GoliathLogger.info(log, methodName, "The returnde row is--->"+row);

				kf.insertRow(st.nextToken(), st.nextToken(), st.nextToken());

				log.info(methodName+":"+"Row inserted to kafka Queue...");
				GoliathLogger.info(log, methodName, "Row inserted to kafka Queue...");
			}
		}
		GoliathLogger.end(log, methodName);
		return null;
	}

	/**
	 *  closes the Kafka Hanldes
	 */
	public void close()
	{
		final String methodName = "close";
		GoliathLogger.start(log, methodName);

		kf.closeKafkaFluser();

		log.info(methodName+":"+"Closing Kafka Flusher...!!");
		GoliathLogger.info(log, methodName, "Closing Kafka Flusher...!!");
		GoliathLogger.end(log, methodName);
	}
}
