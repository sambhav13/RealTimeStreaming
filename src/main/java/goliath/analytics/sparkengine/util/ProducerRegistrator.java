package goliath.analytics.sparkengine.util;

import goliath.analytics.constants.ApplicationConstants;
import goliath.analytics.errorHandling.GoliathBaseException;
import goliath.analytics.logging.GoliathLogger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.producer.ProducerConfig;

import org.apache.avro.AvroRemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.globallogic.goliath.platform.platformservices.PlatformServicesClient;
import com.globallogic.goliath.platform.platformservices.model.Domain;
import com.globallogic.goliath.platform.platformservices.services.PlatformServices;


/**
 * Handles the Producer registration and retrieval
 * @author sambhav.gupta
 *
 */
@Component("pR")
public class ProducerRegistrator {

    private kafka.javaapi.producer.Producer<String, String> producer = null;

    private List<kafka.javaapi.producer.Producer<String, String>> producers = 
    		new ArrayList<kafka.javaapi.producer.Producer<String, String>>();

    private Map<String, kafka.javaapi.producer.Producer<String, String>> domainProducerMapper = null;

    private static final Logger log = LoggerFactory.getLogger(ProducerRegistrator.class);

    private List<Domain> domainColl = null;


    @Value("${PLATFORM_SERVICES_HOSTNAME}")
	private String platformCoreCouchDbInstanceIp;
	
	@Value("${PLATFORM_SERVICES_PORT}")
	private int platformCoreCouchDbInstancePort;
	
   
    /**
     * Constructor
     */
    public ProducerRegistrator()
    {
    	///System.out.println("Product registrator initialized");
    }
        
    /**
     * Registers producers for a given no. of domains
     * @param properties
     * 
     */
    public void registerProducers(Properties properties) {

        final String methodName = "RegisterProducers";
        GoliathLogger.start(log, methodName);
        ProducerConfig producerConfig = new ProducerConfig(properties);
        domainColl = loadDomainIdColl();

        for (int i = 0; i < domainColl.size(); i++) {
            producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
            producers.add(producer);
        }
       
        GoliathLogger.info(log, methodName, "Kafka Producers Registered...");

    }

    /**
     * Loads the respective services handles and returns the Domain Lists
     * @return List of domains
     */
    public List<Domain> loadDomainIdColl() {
        String methodName = "loadUnifiedSchema";
        GoliathLogger.start(log, methodName);

        PlatformServicesClient platformServicesClient = null;
        PlatformServices proxy = null;
        String tenantId = null;
        try {
            platformServicesClient = new PlatformServicesClient();
            proxy = platformServicesClient.getClient(platformCoreCouchDbInstanceIp, platformCoreCouchDbInstancePort);
            String tenantName = ApplicationConstants.tenantName; 
            tenantId = proxy.Tenant_getTenantIdByName(tenantName, ApplicationConstants.ServiceSecretKey).toString();

        }
        catch (IOException ioe) {
            log.error(ioe.getMessage(), ioe);
            throw new GoliathBaseException(ioe.getMessage(), "loadUnifiedSchema");
        }

        List<Domain> domain = null;
        try {
            domain = proxy.Domain_getDomains(tenantId);

        }
        catch (AvroRemoteException are) {
            log.error(are.getMessage(), are);
            throw new GoliathBaseException(are.getMessage(), "loadtenantDomainSchemas");
        }

        System.out.println(methodName + ":" + "Domain Loaded!!");
        log.info(methodName + ":" + "Domain Specific schema mapping loaded...!!");
        GoliathLogger.info(log, methodName, "Domain Specific schema mapping loaded...!!");
        GoliathLogger.end(log, methodName);
        platformServicesClient.closeClient();
        return domain;
    }

    /**
     * Maps the kafka producers for the domain 
     */
    public void mapProducers()

    {

        final String methodName = "MapProducers";
        GoliathLogger.start(log, methodName);

        domainProducerMapper = new HashMap<String, kafka.javaapi.producer.Producer<String, String>>();
       
        for (int i = 0; i < domainColl.size(); i++) {
            domainProducerMapper.put(domainColl.get(i).id.toString(), producers.get(i));
        }

        GoliathLogger.info(log, methodName, "Kafka Producers Mapped with Respective Domains...");
    }

    /**
     * gets the respective Producer for that domain
     * @param domainId
     * @return Domain Specific producer
     */
    public kafka.javaapi.producer.Producer<String, String> getProducer(String domainId) {
        return domainProducerMapper.get(domainId);
    }

    
    /**
     *  Closes the Producer handles
     */
    public void closeProducers() {
      
    	final String methodName = "closeProducers";
        GoliathLogger.start(log, methodName);
        for (kafka.javaapi.producer.Producer<String, String> tempProducer : producers) {
        	tempProducer.close();
         }
        GoliathLogger.info(log, methodName, "Closing Kafka Producers...");
    }
}
