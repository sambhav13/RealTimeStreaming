package goliath.analytics.controller;


import java.util.List;


import goliath.analytics.constants.ApplicationConstants;
import goliath.analytics.constants.AvroSchemaConstants;

import goliath.analytics.helper.DomainModelHelper;
import goliath.analytics.logging.GoliathLogger;
import goliath.analytics.parser.DomainModelParser;

import goliath.analytics.util.DomainObjectLoader;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Component;

/**
 * Controls the domain Observation processing 
 * @author sambhav.gupta
 *
 */
@Component("domainController")
public class DomainModelController {

	@Autowired
	private DomainModelHelper helper;

	@Autowired
	private DomainModelParser modelParser;

	@Autowired
	private  DomainObjectLoader ol;

	private static final Logger log = LoggerFactory.getLogger(DomainModelController.class);
	
	
	/**
	 *  Constructor
	 */
	public DomainModelController()
	{		
		
	}

	
	/**
	 *  loads the row oders details from Couch Db
	 */
	public void loadRowOder()
	{
		
		ol.loadRowOrderDetailsFromCouchDb();
	}
	
	/**
	 *  loads the helper handles
	 */
	public void loadHelper()
	{
		helper.loadUnifiedSchema();
		
	}

	
	/**
	 * Gets the Processed rows from domain Observation
	 * @param domainModelObservation
	 * @param processingEngine
	 * @return processedObservationData for hdfs/kafka
	 */
	public String getProcessedData(byte[] domainModelObservation, String processingEngine) {
		
		String methodName = "getDomainModelOfObservation";
		GoliathLogger.start(log, methodName);

		Schema commonSchema =helper.getCommonSchema();
		
		System.out.println("common Schema"+commonSchema);
		GenericRecord observation = modelParser.parseDomainModelObservation(
				domainModelObservation, commonSchema);

		GenericRecord context = (GenericRecord) observation.get(AvroSchemaConstants.genericRecordContext);
		String domainId = ((Utf8) context.get(AvroSchemaConstants.genericRecordDomainId)).toString();

		Schema domainSchema = helper.getDomainSchema(domainId);
		List<Field> obsFields = (domainSchema.getField(AvroSchemaConstants.genericRecObservations).
				schema().getTypes().get(1).getFields());
		List<Field> metaFields = domainSchema.getField(AvroSchemaConstants.genericRecMetaData).
				schema().getFields();

		log.info(methodName + ":" + "getting Domain Model Observation...!!");
		GoliathLogger.info(log, methodName, "getting Domain Model Observation...!!");
		GoliathLogger.end(log, methodName);

		String processedObservationData = null;
		if(processingEngine.equals(ApplicationConstants.SparkEngine))
		{
			processedObservationData = ol.getProObsData(observation, obsFields, metaFields, domainId);
			System.out.println("The processed observation is----->"+processedObservationData);
			System.out.println("**************\n**************\n**************\n");
			String kafkaTopic =null;

			kafkaTopic = helper.getkafkaTopic(domainId);
			return processedObservationData + "##" + kafkaTopic + "##" + domainId;
		}

		else {

			processedObservationData = ol.getProObsData(observation, obsFields, metaFields, domainId);
			System.out.println("The processed observation is----->"+processedObservationData);
			System.out.println("**************\n**************\n**************\n");
			return processedObservationData;
		}

	}
}
