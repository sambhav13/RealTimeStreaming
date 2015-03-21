package goliath.analytics.helper;

import goliath.analytics.constants.ApplicationConstants;
import goliath.analytics.constants.AvroSchemaConstants;
import goliath.analytics.errorHandling.GoliathBaseException;
import goliath.analytics.logging.GoliathLogger;
import goliath.analytics.util.DomainObjectLoader;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.globallogic.goliath.platform.platformservices.PlatformServicesClient;
import com.globallogic.goliath.platform.platformservices.model.Domain;
import com.globallogic.goliath.platform.platformservices.services.PlatformServices;


/**
 * Helps in loading and prociding Domain and tenant Specific Schema
 * @author sambhav.gupta
 *
 */
@Component("helper")
public class DomainModelHelper {

	@Autowired
	private DomainObjectLoader tm;

	private Schema commonSchema = null;

	private Map<String, Schema> domainSchemaColl = null;

	private String sysAttSchemaFile = ApplicationConstants.sysAttSchemaFile;

	private String cntxtSchemaFile = ApplicationConstants.cntxtSchemaFile;

	private Map<String, String> domainIdKafkaMapper = null;



	@Value("${PLATFORM_SERVICES_HOSTNAME}")
	private String platformCoreCouchDbInstanceIp;
	
	@Value("${PLATFORM_SERVICES_PORT}")
	private int platformCoreCouchDbInstancePort;
	

	private static final Logger log = LoggerFactory.getLogger(DomainModelHelper.class);



	/**
	 *  Constructor
	 */
	public DomainModelHelper()
	{
		
		domainSchemaColl = new HashMap<String, Schema>();
		
	}

	/**
	 * Gets the Domain Specific Domain Schema
	 * @param domainId
	 * @return The domain specific Schema
	 */
	public Schema getDomainSchema(String domainId) {

		Schema domSch = domainSchemaColl.get(domainId);
		return domSch;

	}


	/**
	 * Gets the Common Domains Schema
	 * @return The tenant unified comman Schema
	 */
	public Schema getCommonSchema()
	{

		return this.commonSchema;
	}

	
	/**
	 *  loads the unified Schema for the tenant
	 */
	public void loadUnifiedSchema() {
		final String methodName = "loadUnifiedSchema";
		GoliathLogger.start(log, methodName);

		PlatformServicesClient platformServicesClient = null;
		PlatformServices proxy = null;
		String tenantId = null;
		platformServicesClient = new PlatformServicesClient();
	
		
		System.out.println("Inside loadUnifiedSchema ");
		try {
			proxy = platformServicesClient.getClient(
					platformCoreCouchDbInstanceIp, platformCoreCouchDbInstancePort);

			String tenantName = ApplicationConstants.tenantName;
			tenantId = proxy.Tenant_getTenantIdByName(
					tenantName, ApplicationConstants.ServiceSecretKey).toString();
			loadtenantDomainSchemas(tenantId, proxy);

			platformServicesClient.closeClient();

		}
		catch (NumberFormatException nfe) {
			nfe.printStackTrace();
			log.error(nfe.getMessage(), nfe);
			throw new GoliathBaseException(nfe.getMessage(), "loadUnifiedSchema");

		}
		catch (IOException ioe) {
			ioe.printStackTrace();
			log.error(ioe.getMessage(), ioe);
			throw new GoliathBaseException(ioe.getMessage(), "loadUnifiedSchema");
		}

		///System.out.println(methodName + ":" + "Tenant Unified Schema Loaded..!!");
		log.info(methodName + ":" + "Tenant Unified Schema Loaded..!!");
		GoliathLogger.info(log, methodName, "Tenant Unified Schema Loaded..!!");
		GoliathLogger.end(log, methodName);

	}

	/**
	 * Loads the kafka topics and the domain specific schema in Hashmaps
	 * @param tenantID
	 * @param proxy
	 * 
	 */
	private void loadtenantDomainSchemas(String tenantID, PlatformServices proxy) {
		final String methodName = "loadtenantDomainSchemas";
		GoliathLogger.start(log, methodName);

		InputStream syaAttrStream = getClass().getClassLoader().getResourceAsStream(sysAttSchemaFile);
		InputStream contextStream = getClass().getClassLoader().getResourceAsStream(cntxtSchemaFile);

		domainIdKafkaMapper = new HashMap<String, String>();
		System.out.println("Inside loadtenantDomainSchemas ");
		List<Domain> domain;
		List<Schema> schemas = new ArrayList<Schema>();
		List<Schema> unionOfSchema = new ArrayList<Schema>();
		try {
			domain = proxy.Domain_getDomains(tenantID);

			for (int i = 0; i < domain.size(); i++) {
				com.globallogic.goliath.platform.platformservices.model.DomainModel domainModel = 
						proxy.DomainModel_getDomainModel(tenantID, domain.get(i).id);
				GenericArray struct = (GenericArray) domainModel.get(
						AvroSchemaConstants.genericRecStruct);
				String domainObjectSchemaString = struct.get(0).toString();
				Schema.Parser parser = new Schema.Parser();
				try {

					parser.parse(syaAttrStream);
					parser.parse(contextStream);
				}
				catch (IOException ioe) {
					log.error(ioe.getMessage(), ioe);
					throw new GoliathBaseException(ioe.getMessage(), "loadtenantDomainSchemas");
				}
				Schema domainObject = parser.parse(domainObjectSchemaString);
				domainSchemaColl.put(domain.get(i).id.toString(), domainObject);
				schemas.add(domainObject);
				unionOfSchema.add(domainObject);
				String tableName = "pa_" +ApplicationConstants.tenantName + "_" +
				domain.get(i).name + "_" + domainModel.name;
				domainIdKafkaMapper.put(domain.get(i).id.toString(), tableName.toLowerCase());
			}
			this.commonSchema = Schema.createUnion(unionOfSchema);


			System.out.println(methodName + ":" + "Domain Specific schema mapping loaded...!!");
			GoliathLogger.info(log, methodName, "Domain Specific schema mapping loaded...!!");
			GoliathLogger.end(log, methodName);

		}
		catch (AvroRemoteException are) {
			log.error(are.getMessage(), are);
			throw new GoliathBaseException(are.getMessage(), "loadtenantDomainSchemas");
		}
	}

	/**
	 * Gets the Kafka Topic of specific domain
	 * @param domainId
	 * @return The domain specific Topic
	 */
	public String getkafkaTopic(String domainId) {
		final String methodName = "getkafkaTopic";
		GoliathLogger.start(log, methodName);

		String kafkaTopic = domainIdKafkaMapper.get(domainId);

		log.info(methodName + ":" + "Getting kakfa Topic...!!");
		GoliathLogger.info(log, methodName, "Getting kakfa Topic...!!");
		GoliathLogger.end(log, methodName);
		return kafkaTopic;
	}
}
