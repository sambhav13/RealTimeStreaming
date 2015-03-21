package goliath.analytics.process;

import goliath.analytics.constants.ApplicationConstants;
import goliath.analytics.controller.DomainModelControllerTest;
import goliath.analytics.errorHandling.GoliathBaseException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.BeforeClass;

public class EventWriterTest {
	
	private EventWriter ew ;
	private String propFileName = ApplicationConstants.testPropertyFileName;
	private Properties prop = new Properties();
	private static final Logger log = LoggerFactory.getLogger(EventWriterTest.class);

  @BeforeMethod
  public void beforeMethod() {
  }


  
  @BeforeClass
  public void beforeClass() {
	  
	  ew = new EventWriter(); 
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

  
  @DataProvider(name="test1")
  public Object[][] flumeInitTestData() {
	 		  
	 String flumeinstanceIP = (String) prop.get("FlumeInstanceIp");
	 int flumeInstancePort = Integer.parseInt( prop.get("FlumeInstancePort").toString());
	 
	 
	 
	 return new Object[][] {
		      new Object[] { flumeinstanceIP,flumeInstancePort}};
	 
    /*return new Object[][] {
      new Object[] { "172.22.95.105",5148}};*/
    
  }
  
  @DataProvider(name="test2")
  public Object[][] FlumeInstanceTestData() {
	 		  
    return new Object[][] {
      new Object[] { "KA03HU8885,Sonia,WalmartTruckKA03HU8885,50.0,50.0,50.0,50.0,50.0,2015-02-16T15:55:26.910Z,1419139186871"}};
    
  }
  
  @Test(groups = "init",dataProvider = "test1")
  public void init(String ipAddress,int portNo) {
	  
	  ew.init(ipAddress,portNo);
	  //ew.init("172.22.95.105", 5148);
  }

  @Test(dependsOnGroups = "init",dataProvider = "test2")
  public void forwardParsedDataToFlumeInstance(String parsedData) {
	  
	ew.forwardParsedDataToFlumeInstance(parsedData);
    //ew.forwardParsedDataToFlumeInstance("KA03HU8885,Sonia,WalmartTruckKA03HU8885,50.0,50.0,50.0,50.0,50.0,2014-12-21T05:19:46:871Z,1419139186871");
  }
  
  @Test(dependsOnGroups = "init")
  public void cleanUp() {
  ew.cleanUp();
  }

 

 
}
