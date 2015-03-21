package goliath.analytics.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import goliath.analytics.constants.ApplicationConstants;
import goliath.analytics.errorHandling.GoliathBaseException;
import goliath.analytics.util.FlumeDomainModelObservation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DomainModelControllerTest {
	
	private DomainModelController dmcObj = null;
	private FlumeDomainModelObservation fdm = null;
	private String propFileName = ApplicationConstants.testPropertyFileName;
	private Properties prop = new Properties();
	private static final Logger log = LoggerFactory.getLogger(DomainModelControllerTest.class);
	
  @Test(dataProvider = "dp")
  public void f(Integer n, String s) {
  }
  @BeforeMethod
  public void beforeMethod() {
  }

  @AfterMethod
  public void afterMethod() {
  }


  @DataProvider
  public Object[][] dp() {
    return new Object[][] {
      new Object[] { 1, "a" },
      new Object[] { 2, "b" },
    };
  }
  
  
 
  @BeforeClass
  public void beforeClass() {
	  
	  ApplicationContext appContext = new ClassPathXmlApplicationContext("applicationContext.xml");
	  dmcObj = (DomainModelController) appContext.getBean("domainController");
	  //dmcObj = new DomainModelController();
	  //dmcObj.loadHandles();
	  dmcObj.loadRowOder();
	  dmcObj.loadHelper();
	  
	  fdm = new FlumeDomainModelObservation();
	  fdm.configureProperties();
	  fdm.loadUnifiedSchema();
	  
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

  @AfterClass
  public void afterClass() {
  }

  @BeforeTest
  public void beforeTest() {
  }

  @AfterTest
  public void afterTest() {
  }

  @BeforeSuite
  public void beforeSuite() {
  }

  @AfterSuite
  public void afterSuite() {
  }

  
 
  
  @DataProvider(name="test1")
  public Object[][] DomainModelTestData() {
	 		  
	  String flumeinstanceIP = (String) prop.get("FlumeInstanceIp");
	  String flumeInstancePort = (String) prop.get("FlumeInstancePort");
	 
	  return new Object[][] {
		      new Object[] { ""+flumeinstanceIP+","+flumeInstancePort+",KA03HU8885,Sonia,WalmartTruckKA03HU8885,50,2015-02-16T15:55:26.910Z"}};
	
  }
  @Test(dataProvider = "test1")
  public void getDomainModelOfObservation(String argList) {
	  
	 
	 
	  
	  List<String> items = Arrays.asList(argList.split("\\s*,\\s*"));
	  final String args[] = (String[])items.toArray();
	    
	  
	  byte b[] =fdm.createDomainObject(args); 
	  String hdfsRow  = dmcObj.getProcessedData(b,ApplicationConstants.FlumeEngine);
	 
	  System.out.println("Row to be dumped to hdfs");
	 
	  Assert.assertEquals("KA03HU8885,Sonia,WalmartTruckKA03HU8885,50.0,50.0,50.0,50.0,50.0,2015-02-16T15:55:26.910Z,1424102126910",hdfsRow);	  
	  
	  String sparkRow  = dmcObj.getProcessedData(b,ApplicationConstants.SparkEngine);
		 
	  System.out.println("Row to be dumped to hdfs");
	
	  Assert.assertEquals("KA03HU8885,Sonia,WalmartTruckKA03HU8885,50.0,50.0,50.0,50.0,50.0,2015-02-16T15:55:26.910Z,1424102126910##pa_walmart_transcare_truck##2d131cdd-2c73-4be4-88d5-5868937b6518",sparkRow);
	  
  }

  @Test
  public void loadHandles() {

  }
}