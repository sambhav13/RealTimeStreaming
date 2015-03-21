package goliath.analytics.sparkengine.dao;


import junit.framework.Assert;
import kafka.javaapi.producer.Producer;

import goliath.analytics.sparkengine.dao.KafkaSink;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.BeforeClass;


@Test
@ContextConfiguration(locations = { "classpath:applicationContext.xml" })
public class KafkaSinkTest extends AbstractTestNGSpringContextTests{

	@Autowired
	private KafkaSink kfs;

	@Test(dataProvider = "dp")
	public void f(Integer n, String s) {
	}
	@BeforeMethod
	public void beforeMethod() {
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

	}


	@Test
	public void KafkaSink() {



	}

	
	@DataProvider(name="test1")
	  public Object[][] getProducerTestData() {
		 		  
	    return new Object[][] {
	      new Object[] { "2d131cdd-2c73-4be4-88d5-5868937b6518","2d131cdd-2c73-4be4-88d5-5868937b651"}};
	    
	  }
	
	@Test(dependsOnGroups = "init")
	public void closeKafkaFluser() {
		kfs.closeKafkaFluser();
	}

	@Test(dependsOnGroups = "init",dataProvider = "test1")
	public void getProducer(String domainId,String failDomainId) {
		
		
		Producer p =kfs.getProducer(domainId);
		Producer p2 =kfs.getProducer(failDomainId);
		Assert.assertNotNull(p);
		Assert.assertNull(p2);
		
	}

	@Test(groups = "init")
	public void init() {
		kfs.init();
	}

	@DataProvider(name="test2")
	  public Object[][] insertTestData() {
		 		  
	    return new Object[][] {
	      new Object[] { "KA03HU8866,John Doe,WalmartTruckKA03HU8866,35.0,103.0,249.99120219999895,74.77482,26.41774,2015-02-16T15:55:26.910Z,1423039904255","2d131cdd-2c73-4be4-88d5-5868937b6518","pa_walmart_transcare_truck"}};
	    
	  }
	
	@Test(dataProvider = "test2")
	public void insert(String row,String domainId,String topic) {
		
		
		kfs.insertRow(row,topic,domainId);
	}
}
