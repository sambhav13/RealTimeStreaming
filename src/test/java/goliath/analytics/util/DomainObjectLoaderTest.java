package goliath.analytics.util;

import goliath.analytics.constants.ApplicationConstants;
import goliath.analytics.constants.AvroSchemaConstants;
import goliath.analytics.controller.DomainModelControllerTest;
import goliath.analytics.errorHandling.GoliathBaseException;
import goliath.analytics.helper.DomainModelHelper;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
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
public class DomainObjectLoaderTest extends  AbstractTestNGSpringContextTests {


	@Autowired
	private DomainObjectLoader domObjLoad;
	
	@Autowired
	private DomainModelHelper helper;

	private FlumeDomainModelObservation fdm = null;
	
	private String propFileName = ApplicationConstants.testPropertyFileName;
	private Properties prop = new Properties();
	private static final Logger log = LoggerFactory.getLogger(DomainModelControllerTest.class);

	
	@BeforeMethod
	public void beforeMethod() {
	}

	
	@BeforeClass
	public void beforeClass() {

		fdm = new FlumeDomainModelObservation();
		fdm.configureProperties();
		fdm.loadUnifiedSchema();
		
		helper.loadUnifiedSchema();
		domObjLoad.loadRowOrderDetailsFromCouchDb();
		
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
	public void DomainObjectLoader() {

	}

	@Test(expectedExceptions = { ParseException.class })
	public void getISOTimeStamp() throws ParseException {


		domObjLoad.getISOTimeStamp("2015-02-04");
		throw new ParseException("Unparseable date: 2015-02-04",004);


	}

	
	 @DataProvider(name="test1")
	  public Object[][] testData() {
		 		  
		String flumeinstanceIP = (String) prop.get("FlumeFirstInstanceIP");
		String flumeInstancePort = (String) prop.get("FlumeFirstInstancePort");
		 
		 
	    return new Object[][] {
	      new Object[] { ""+flumeinstanceIP+","+flumeInstancePort+",KA03HU8885,Sonia,WalmartTruckKA03HU8885,50,2015-02-16T15:55:26.910Z","2d131cdd-2c73-4be4-88d5-5868937b6518"}};
	    
	  }
	
	@Test(dataProvider = "test1")
	public void getProObsData(String argList,String domainId) {

		
		List<String> items = Arrays.asList(argList.split("\\s*,\\s*"));
		final String args[] = (String[])items.toArray();		
			  
		GenericRecord parsedObservation =  fdm.getSampleGenericRecord(args);
		
		
		Schema domainSchema = helper.getDomainSchema("2d131cdd-2c73-4be4-88d5-5868937b6518");
		
		List<Field> obsFields = (domainSchema.getField(AvroSchemaConstants.genericRecObservations).
				schema().getTypes().get(1).getFields());
		List<Field> metaFields = domainSchema.getField(AvroSchemaConstants.genericRecMetaData).
				schema().getFields();
	
		
		domObjLoad.getProObsData(parsedObservation, obsFields, metaFields, "2d131cdd-2c73-4be4-88d5-5868937b6518");
	}

	@Test
	public void getRowSequence() {

	}

	@Test
	public void loadDomainIdColl() {

	}

	@Test
	public void loadDomainRowSequences() {

	}

	@Test
	public void loadRowOrderDetailsFromCouchDb() {

	}
}
