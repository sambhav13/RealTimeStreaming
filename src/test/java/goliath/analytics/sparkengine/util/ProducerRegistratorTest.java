package goliath.analytics.sparkengine.util;


import goliath.analytics.constants.ApplicationConstants;
import goliath.analytics.controller.DomainModelControllerTest;
import goliath.analytics.errorHandling.GoliathBaseException;
import goliath.analytics.sparkengine.dao.KafkaSink;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.BeforeClass;

@Test
@ContextConfiguration(locations = { "classpath:applicationContext.xml" })
public class ProducerRegistratorTest extends AbstractTestNGSpringContextTests{


	@Autowired
	private ProducerRegistrator pR;
	
	@Autowired
	private KafkaSink kfs;

	private Properties kafkaProperties = null;
	
	private String propFileName = ApplicationConstants.testPropertyFileName;
	private Properties prop = new Properties();
	private static final Logger log = LoggerFactory.getLogger(ProducerRegistratorTest.class);

	
	@BeforeMethod
	public void beforeMethod() {
	}

	
	@BeforeClass
	public void beforeClass() {
		
		 InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

		 if (inputStream != null) {
			 try {
				 prop.load(inputStream);
			 } catch (IOException e) {

				 log.error("IOException ", e);
				 throw new GoliathBaseException("IO Exception ","beforeClass");
				
			 }
		 } else {
			 try {

				 throw new GoliathBaseException("property file '" + propFileName + "' not found in the classpath","configure");

			 } catch (GoliathBaseException gbex) {

				 log.error("property file '" + propFileName + "' not found in the classpath", gbex);
				 throw new GoliathBaseException("property file '" + propFileName + "' not found in the classpath","configure");
			 }
		 }
	}


	@Test
	public void ProducerRegistrator() {
		
		
		
	}

	@Test
	public void closeProducers() {

	}

	@Test
	public void getProducer() {

	}

	@Test
	public void loadDomainIdColl() {

	}

	@DataProvider(name="registerProducers")
	  public Object[][] registerProducersTestData() {
		 		  
		String kafkaBroker = (String) prop.get("kafkaBrokerList");
		
		
		return new Object[][] {
			      new Object[] { kafkaBroker}};
		    
	  }
	
	@Test(dependsOnGroups = "init")
	public void mapProducers() {

		pR.mapProducers();
	}

	@Test(groups = "init",dataProvider = "registerProducers")
	public void registerProducers(String brokerList) {

		kafkaProperties = new Properties();
		kafkaProperties.put(ApplicationConstants.metaBrokerList_PropName,brokerList);
		kafkaProperties.put(ApplicationConstants.serializerClass_PropName,ApplicationConstants.kafka_SerializerClass);
		pR.registerProducers(kafkaProperties);
	}
}
