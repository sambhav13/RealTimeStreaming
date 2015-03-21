package goliath.analytics.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;


import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.globallogic.goliath.platform.platformservices.PlatformServicesClient;

import com.globallogic.goliath.platform.platformservices.model.Domain;
import com.globallogic.goliath.platform.platformservices.services.PlatformServices;


import goliath.analytics.constants.ApplicationConstants;
import  goliath.analytics.constants.DomainModelConstants;
import goliath.analytics.errorHandling.GoliathBaseException;
import goliath.analytics.logging.GoliathLogger;
import goliath.analytics.process.DomainSink;
import goliath.analytics.sparkengine.goliath.analytics.constants.AvroSchemaConstants;

public class FlumeDomainModelObservation {

	/**
	 * @param args
	 */

	private Schema commonSchema = null;
	private Map<String,List<Schema>> tenantSchmaCol = new HashMap<String,List<Schema>>();
	private Map<String,Schema> domainSchemaColl = new HashMap<String,Schema>();
	private Schema truckSchema = null;


	private DecoderFactory DECODER_FACTORY = new DecoderFactory();
	private String JSON_CONTENT_TYPE = "application/json";
	private final String BINARY_CONTENT_TYPE = "avro/binary";

	private PlatformServicesClient platformServicesClient = null;
	private PlatformServices proxy = null;

	private String truckSchemaFile = ApplicationConstants.truckSchemaFile;
	private String sysAttSchemaFile = ApplicationConstants.sysAttSchemaFile;
	private String cntxtSchemaFile = ApplicationConstants.cntxtSchemaFile;
	private Schema contxtSchema = null;
	private Schema sysAttrSchema  = null;
	private Schema metaDataSchema = null;
	private Schema obsSchema = null;
	private InputStream inputStream2 = null;
	private InputStream inputStream3 = null;

	private String propFileName = ApplicationConstants.propertyFileName;	
	private Properties prop = new Properties();

	private static final Logger log = LoggerFactory.getLogger(DomainSink.class);
	public static void main(String[] args) {

		FlumeDomainModelObservation fdm = new FlumeDomainModelObservation();
		fdm.configureProperties();
		fdm.getplatformCoreServiceModels(args);
	}


	public void configureProperties() {
		String methodName = "configureProperties";
		GoliathLogger.start(log, methodName);

		InputStream inputStream1 = getClass().getClassLoader().getResourceAsStream(truckSchemaFile);
		inputStream2 = getClass().getClassLoader().getResourceAsStream(sysAttSchemaFile);
		inputStream3 = getClass().getClassLoader().getResourceAsStream(cntxtSchemaFile);
		Schema.Parser parser = new Schema.Parser();
		try {
			sysAttrSchema =parser.parse(inputStream2);
			contxtSchema = parser.parse(inputStream3);
			truckSchema = parser.parse(inputStream1);
		} catch (IOException ioe) {
			log.error(ioe.getMessage(), ioe);
			throw new GoliathBaseException(ioe.getMessage(),"configureProperties");
		}
		GoliathLogger.info(log, methodName, "Properties Configured..!!");
		GoliathLogger.end(log, methodName);
	}

	public void  loadUnifiedSchema()
	{
		final String methodName = "loadUnifiedSchema";
		GoliathLogger.start(log, methodName);

		String tenantId = null;
		try{
			platformServicesClient = new PlatformServicesClient();
			proxy = platformServicesClient.getClient(prop.getProperty(ApplicationConstants.ServiceDBInstanceIp),Integer.parseInt(prop.getProperty(ApplicationConstants.ServiceDBInstancePort)));
			String tenantName = "walmart"; //Put this tenant name in the conf file

			GoliathLogger.info(log, methodName,"Inside platform2");
			
			tenantId = proxy.Tenant_getTenantIdByName(tenantName,ApplicationConstants.ServiceSecretKey).toString(); //Please use this "secret_key" key for this api
			
			GoliathLogger.info(log, methodName,"Inside platform3");
			GoliathLogger.info(log, methodName,"Tenant id : " + tenantId);
			
			loadtenantDomainSchemas(tenantId);
			loadtenantUnifiedSchema();

			platformServicesClient.closeClient();
			
			GoliathLogger.info(log, methodName, "Tenant Unified Schema Loaded..!!");
			GoliathLogger.end(log, methodName);
		} catch (NumberFormatException nfe) {
			log.error(nfe.getMessage(), nfe);
			throw new GoliathBaseException(nfe.getMessage(),"loadUnifiedSchema");
		} catch (IOException ioe) {
			log.error(ioe.getMessage(), ioe);
			throw new GoliathBaseException(ioe.getMessage(),"loadUnifiedSchema");
		} 
	}

	private void loadtenantUnifiedSchema()
	{
		List<Schema> allTentSchemas = new  ArrayList<Schema>(); 
		for(Entry<String,List<Schema>> tenId : tenantSchmaCol.entrySet())
		{
			List<Schema> tenantDomainSchemas =  tenId.getValue();
			for(Schema sc : tenantDomainSchemas)
			{
				allTentSchemas.add(sc);
			}
		}
		commonSchema = Schema.createUnion(allTentSchemas);
	}

	private void  loadtenantDomainSchemas(String tenantID)
	{
		final String methodName = "loadtenantDomainSchemas";
		GoliathLogger.start(log, methodName);

		List<Schema> scColl = new ArrayList<Schema>();
		List<Domain> domain = null;
		try {
			domain = proxy.Domain_getDomains(tenantID);
			GoliathLogger.info(log, methodName, "the truck schema -->"+truckSchema);
			InputStream syaAttrStream = getClass().getClassLoader().getResourceAsStream(sysAttSchemaFile);
			InputStream contextStream = getClass().getClassLoader().getResourceAsStream(cntxtSchemaFile);
			for(int i=0;i<domain.size();i++)
			{
				com.globallogic.goliath.platform.platformservices.model.DomainModel domainModel = proxy.DomainModel_getDomainModel(tenantID, domain.get(i).id);
				GenericArray  struct = (GenericArray)domainModel.get(AvroSchemaConstants.genericRecStruct);
				String domainObjectSchemaString  = struct.get(0).toString();
				Schema.Parser parser = new Schema.Parser();
				try {
					parser.parse(syaAttrStream);				
					parser.parse(contextStream);
				} catch (IOException ioe) {
					log.error(ioe.getMessage(), ioe);
					throw new GoliathBaseException(ioe.getMessage(),"loadtenantDomainSchemas");
				}
				Schema domainObject  = parser.parse(domainObjectSchemaString);
				
				GoliathLogger.info(log, methodName, "The domain id is---->"+domain.get(i).id.toString());
				domainSchemaColl.put(domain.get(i).id.toString(), domainObject);
				scColl.add(domainObject);
			}

			tenantSchmaCol.put(tenantID.toString(),scColl);
			
			GoliathLogger.info(log, methodName, "Domain Schemas for tenant  : " + tenantID+" loaded");

			com.globallogic.goliath.platform.platformservices.model.DomainModel domainModel = proxy.DomainModel_getDomainModel(tenantID, domain.get(0).id); 
		
			GoliathLogger.info(log, methodName, "Domain model schema : " + domainModel.getSchema());
			GoliathLogger.info(log, methodName, "Domain Specific schema mapping loaded...!!");
			GoliathLogger.end(log, methodName);
		} catch (AvroRemoteException are) {
			log.error(are.getMessage(), are);
			throw new GoliathBaseException(are.getMessage(),"loadtenantDomainSchemas");
		}

	}

	public static byte[] avroSerialize(GenericRecord serializableObject, Schema schema) {
		
		byte[] avroBytes = null;
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
		DatumWriter writer = new GenericDatumWriter(schema);
		try {
				writer.write(serializableObject, encoder);
				encoder.flush();
		} catch (IOException ioe) {
			log.error(ioe.getMessage(), ioe);
			throw new GoliathBaseException(ioe.getMessage(),"avroSerialize");
		}
		avroBytes = outputStream.toByteArray();
		return avroBytes;
	}

	public static GenericRecord avroDeserialize(byte[] avroBytes, Schema schema) {

		GenericRecord ret = null;
		try {
			ByteArrayInputStream in = new ByteArrayInputStream(avroBytes);
			Decoder d = DecoderFactory.get().directBinaryDecoder(in, null);
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
			ret = reader.read(null, d);
		} catch (IOException ioe) {
			log.error(ioe.getMessage(), ioe);
			throw new GoliathBaseException(ioe.getMessage(),"avroDeserialize");
		}
		return ret;
	}


	private byte[] createDomainObject(String[] args)
	{
		final String methodName = "createDomainObject";
		GoliathLogger.start(log, methodName);
		GoliathLogger.info(log, methodName, "Creating Domain Object...");

		metaDataSchema = truckSchema.getField("metaData").schema();
		obsSchema = truckSchema.getField("observations").schema().getTypes().get(1);

		GenericRecordBuilder truckBuilder;
		truckBuilder = new GenericRecordBuilder(truckSchema);
		GenericRecord sa = new GenericRecordBuilder(sysAttrSchema).build();
		sa.put("createdByUser","John Doe");
		sa.put("creationDate","2014-12-18T08:15:20-05:00");
		sa.put("isActive","true");
		sa.put("isEnabled","true");
		sa.put("lastUpdateUser","John Doe");
		sa.put("objectId","02398901127hkas8yy82");
		sa.put("tags","na");
		sa.put("tenantId","2d131cdd-2c73-4be4-88d5-5868937b65c4");
		sa.put("updatedDate","2014-12-18T08:20:20-05:00");
		truckBuilder.set("systemAttributes",sa);

		GenericRecord context = new GenericRecordBuilder(contxtSchema).build();
		context.put("domainId","2d131cdd-2c73-4be4-88d5-5868937b6518");
		context.put("versionId","1.0");
		truckBuilder.set("context",context);

		GenericRecord tsd = new GenericRecordBuilder(metaDataSchema).build();
		tsd.put("truckNumber",args[2]);
		tsd.put("truckDriver",args[3]);
		tsd.put("resourceGUID",args[4]);

		truckBuilder.set("metaData",tsd);
		truckBuilder.set("entityType","Truck");
		truckBuilder.set("observations", null);
		
		GenericRecord truck =truckBuilder.build();
		Schema truckObservationSchema =null;
		GenericRecord truckObservation =  createTruckObservation(obsSchema,args[5],args[6]);
		truck.put("observations",truckObservation);
		byte[] encodedDataTruck = null;
		encodedDataTruck = avroSerialize(truck,commonSchema);
		
		GoliathLogger.info(log, methodName, " Domain Object created...!!");
		GoliathLogger.end(log, methodName);
		return encodedDataTruck;
	}

	public GenericRecord createTruckObservation(Schema truckObservationSchema,String numericaValue,String timeStamp) {
		GenericRecord truckObservation =null;
		truckObservation = new GenericRecordBuilder(truckObservationSchema).build();
		for (Field field : truckObservationSchema.getFields()) {
			String fieldName = field.name();
			if (field.name().equals(DomainModelConstants.TimeStampField)) {
				truckObservation.put(fieldName, timeStamp);
			} else
				truckObservation.put(fieldName, Double.parseDouble(numericaValue));
		}
		return truckObservation;
	}

	public void getplatformCoreServiceModels(String[] args)
	{
		loadUnifiedSchema();
		byte[] flumeData = createDomainObject(args);
		writetoFlume(flumeData,args);
	}

	public void writetoFlume(byte[] avroData,String[] args)
	{
		
		final String methodName = "writetoFlume";
		GoliathLogger.start(log, methodName);
		GoliathLogger.info(log, methodName, "Sending data to flume Instance...");

		FlumeClient client = new FlumeClient();
		client.init(args[0],Integer.parseInt(args[1])) ;
		Map<String,String> headers = new HashMap<String,String>();
		for (int i = 0; i < 1; i++) {
			headers.put("rowKey",String.valueOf(System.currentTimeMillis()));
			client.sendDataToFlume(avroData,headers);
		}
		client.cleanUp();
		
		GoliathLogger.info(log, methodName, "Data sent  to flume Instance...");
		GoliathLogger.end(log, methodName);
	}
}

class FlumeClient {

	private RpcClient client;
	private String hostname;
	private int port;

	public void init(String hostname, int port) {

		this.hostname = hostname;
		this.port = port;
		this.client = RpcClientFactory.getDefaultInstance(hostname, port);

	}

	public void sendDataToFlume(byte[] data,Map<String,String> headers ) {

		Event event = EventBuilder.withBody(data);
		event.setHeaders(headers);
		try {
			client.append(event);
		} catch (EventDeliveryException e) {
			client.close();
			client = null;
			client = RpcClientFactory.getDefaultInstance(hostname, port);
		}
	}

	public void cleanUp() {
		client.close();
	}
}
