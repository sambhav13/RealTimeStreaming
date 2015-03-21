package goliath.analytics.util;

import goliath.analytics.constants.ApplicationConstants;
import goliath.analytics.constants.AvroSchemaConstants;
import goliath.analytics.constants.DomainModelConstants;
import goliath.analytics.errorHandling.GoliathBaseException;
import goliath.analytics.logging.GoliathLogger;
import goliath.analytics.sparkengine.process.StreamReceiver;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TimeZone;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;
import com.globallogic.goliath.platform.platformservices.PlatformServicesClient;
import com.globallogic.goliath.platform.platformservices.model.Domain;
import com.globallogic.goliath.platform.platformservices.services.PlatformServices;
import org.springframework.stereotype.Component;

/**
 * Prepares the row sequences using the provided Domain Object 
 * @author sambhav.gupta
 *	
 */
@Component("ol")
public class DomainObjectLoader {

	private Properties prop;

	private Map<String, Map<String, Integer>> tenantRowSequenceColl = null;

	private static final Logger log = LoggerFactory
			.getLogger(DomainObjectLoader.class);

	@Value("${PLATFORM_SERVICES_HOSTNAME}")
	private String platformCoreCouchDbInstanceIp;

	@Value("${PLATFORM_SERVICES_PORT}")
	private int platformCoreCouchDbInstancePort;
	
	@Value("${COUCHDB_URL}")
	private String couchdbUrl;

	/**
	 *  Constructor
	 */
	public DomainObjectLoader() {
		///System.out.println("Domain loader Initialized");
		tenantRowSequenceColl = new HashMap<String, Map<String, Integer>>();

	}

	/**
	 * loads the row oder details from couch DB
	 */
	  public void loadRowOrderDetailsFromCouchDb() {

          String methodName = "loadRowOrderDetailsFromCouchDb";
          GoliathLogger.start(log, methodName);

          ArrayList<URI> nodes = new ArrayList<URI>();
          nodes.add(URI.create(couchdbUrl));
          GoliathLogger.info(log, methodName, "the couch db is starting");
          List<Domain> domainColl = loadDomainIdColl();
          CouchbaseClient client = null;
          for (int i = 0; i < domainColl.size(); i++) {
                  try {
                          client = new CouchbaseClient(nodes,
                                          DomainModelConstants.COUCHDB_TENANT_BUCKET_NAME,
                                          DomainModelConstants.COUCHDB_TENANT_BUCKET_PASSWORD);
                          String domainName = null;
                          String domainId = domainColl.get(i).id.toString();
                          View domainMapview = client.getView(DomainModelConstants.COUCHDB_ANALYTICS_MAPPING,
                                          DomainModelConstants.COUCHDB_DOMAIN_VIEW);
                          com.couchbase.client.protocol.views.Query domainNameQuery =
                                          new com.couchbase.client.protocol.views.Query();
                          domainNameQuery.setIncludeDocs(true);
                          domainNameQuery.setKey(domainId);
                          ViewResponse resultDomainName = client.query(domainMapview,
                                          domainNameQuery);
                          for (ViewRow row : resultDomainName) {
                                  domainName = row.getValue();
                          }
                          GoliathLogger.info(log, methodName, "The Domain name  is --->"
                                          + domainName);
                          GoliathLogger.info(log, methodName,
                                          "the couchDB client is setup");

                          loadDomainRowSequences(domainId, client);
                  } catch (IOException io) {
                          log.error("Error connecting to Couchbase:", io);
                          throw new GoliathBaseException(io.getMessage(),
                                          "loadRowOrderDetailsFromCouchDb");
                  }
                  finally{
                	  client.shutdown();
                  }
          }

          System.out.println("Row order details loaded from ...!!");
          GoliathLogger.info(log, methodName,
                          "Row order details loaded from ...!!");
          GoliathLogger.end(log, methodName);
  }

	
	
	public List<Domain> loadDomainIdColl() {

		String methodName = "loadUnifiedSchema";
		GoliathLogger.start(log, methodName);

		PlatformServicesClient platformServicesClient = null;
		PlatformServices proxy = null;
		String tenantId = null;
		List<Domain> domain = null;
		try {
			platformServicesClient = new PlatformServicesClient();
			proxy = platformServicesClient.getClient(platformCoreCouchDbInstanceIp, 
					platformCoreCouchDbInstancePort);
			String tenantName = ApplicationConstants.tenantName;
			///System.out.println("The tenanat Name is " + tenantName);
			tenantId = proxy.Tenant_getTenantIdByName(tenantName, 
					ApplicationConstants.ServiceSecretKey).toString();
			domain = proxy.Domain_getDomains(tenantId);
		} catch (AvroRemoteException are) {
			log.error(are.getMessage(), are);
			throw new GoliathBaseException(are.getMessage(),
					"loadtenantDomainSchemas");
		} catch (IOException ioe) {
			log.error(ioe.getMessage(), ioe);
			throw new GoliathBaseException(ioe.getMessage(),
					"loadUnifiedSchema");
		}

		System.out.println(methodName + ":"	+ "Domain Specific schema mapping loaded...!!");
		GoliathLogger.info(log, methodName,
				"Domain Specific schema mapping loaded...!!");
		GoliathLogger.end(log, methodName);

		platformServicesClient.closeClient();
		return domain;
	}

	/**
	 * Loades the domain row sequences from Db
	 * @param domainId
	 * @param client
	 */
	public void loadDomainRowSequences(String domainId, CouchbaseClient client) {
		String methodName = "loadDomainRowSequences";
		GoliathLogger.start(log, methodName);

		String order = null;
		View orderMapview = client.getView(DomainModelConstants.COUCHDB_ANALYTICS_MAPPING, 
				DomainModelConstants.COUCHDB_ORDER_VIEW);
		com.couchbase.client.protocol.views.Query orderQuery = new com.couchbase.client.protocol.views.Query();
		orderQuery.setIncludeDocs(true);
		orderQuery.setKey(domainId);
		ViewResponse resultOrder = client.query(orderMapview, orderQuery);
		for (ViewRow row : resultOrder) {
			order = row.getValue();
		}

		StringTokenizer columnsBreaker = new StringTokenizer(order, ",");
		Map<String, Integer> rowOrder = new HashMap<String, Integer>();

		int k = 0;
		while (columnsBreaker.hasMoreTokens()) {
			rowOrder.put(columnsBreaker.nextToken(), k);
			k++;
		}
		k = 0;
		tenantRowSequenceColl.put(domainId, rowOrder);

		System.out.println("The Domain Row Sequences are loaded");
		GoliathLogger.info(log, methodName,
				"The Domain Row Sequences are loaded");
		GoliathLogger.end(log, methodName);
	}

	/**
	 * gets the Row Sequence 
	 * @param domainId
	 * @returns the row sequence for persistence
	 */
	public Map<String, Integer> getRowSequence(String domainId) {
		return tenantRowSequenceColl.get(domainId);
	}

	/**
	 * Parses the Domain Observation and processes to return required String
	 * @param parsedObservation
	 * @param obsFields
	 * @param metaFields
	 * @param domainId
	 * @return the processed row
	 */
	public String getProObsData(
			GenericRecord parsedObservation, List<Field> obsFields, List<Field> metaFields,
			String domainId) {
		String methodName = "getParsedDomainModel";
		GoliathLogger.start(log, methodName);
		Object[] columns = new String[obsFields.size() + metaFields.size() + 1];
		Map<String, Integer> liveRowSequence = getRowSequence(domainId);
		GenericRecord recObser = (GenericRecord) parsedObservation.get(
				AvroSchemaConstants.genericRecObservations);
		GenericRecord recMetaData = (GenericRecord) parsedObservation.get(
				AvroSchemaConstants.genericRecMetaData);
		for (int i = 0; i < obsFields.size(); i++) {

			if ((obsFields.get(i).getProp(DomainModelConstants.LogicalType)!=null)) {
				if ((obsFields.get(i).getProp(DomainModelConstants.LogicalType).
						equalsIgnoreCase(DomainModelConstants.TimeStampLogicalFieldValue))) {
				
					if (recObser.get(obsFields.get(i).name()) != null) {
						String timestamp = recObser.get(obsFields.get(i).name())
								.toString();
						Date t = getISOTimeStamp(timestamp);
						columns[liveRowSequence.get(obsFields.get(i).name())] = timestamp;
						columns[liveRowSequence.get(obsFields.get(i).name()+"_msec")] = String
								.valueOf(t.getTime());
					} else {
						columns[liveRowSequence.get(obsFields.get(i).name())] = "";
						columns[liveRowSequence.get(obsFields.get(i).name()+"_msec")] = "";
					}
				}

			} else {
				if (recObser.get(obsFields.get(i).name()) != null) {
					columns[liveRowSequence.get(obsFields.get(i).name())] = recObser
							.get(obsFields.get(i).name()).toString();
				}
				else {
					columns[liveRowSequence.get(obsFields.get(i).name())] = "";
				}
			}
		}
		for (int i = 0; i < metaFields.size(); i++) {
			if (recMetaData.get(metaFields.get(i).name()) != null) {
				columns[liveRowSequence.get(metaFields.get(i).name())] = recMetaData
						.get(metaFields.get(i).name()).toString();
			} else {
				columns[liveRowSequence.get(metaFields.get(i).name())] = "";
			}
		}
		StringBuilder row = getColumnRow(columns);
		
		GoliathLogger.info(log, methodName,
				"The Domain Object is parsed and returned....|!");
		GoliathLogger.end(log, methodName);
		return row.toString();
	}

	/**
	 * 
	 * Gets coorectly formed Columns Row
	 * @param columns
	 * @return
	 */
	public StringBuilder getColumnRow(Object[] columns)
	{
		StringBuilder row = new StringBuilder("");
		for (int i = 0; i < columns.length; i++) {
			row.append(columns[i]);
			if (i != columns.length - 1) { row.append(","); }
		}
		return row;
	}
	
	/**
	 * Gets the ISO timestamp 
	 * @param timestamp
	 * @return
	 */
	public Date getISOTimeStamp(String timestamp)
	{
		TimeZone tz = TimeZone.getTimeZone(
				goliath.analytics.constants.DomainModelConstants.Timezone);
				
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        System.out.println("The time stamp is ------>"+timestamp);
		df.setTimeZone(tz);
		Date t = null;
		try {
			t = df.parse(timestamp);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return t;

	}
		
	  public static void main(String[] args) {
	        final String methodName = "main";
	        GoliathLogger.start(log, methodName);

	        
	        ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
	        DomainObjectLoader streamReceiver = (DomainObjectLoader) context.getBean("ol");
	        streamReceiver.loadRowOrderDetailsFromCouchDb();
	       
	        GoliathLogger.end(log, methodName);
	        
	    }
	
}
