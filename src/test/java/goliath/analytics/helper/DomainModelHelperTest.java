package goliath.analytics.helper;


import goliath.analytics.errorHandling.GoliathBaseException;

import java.io.IOException;
import java.io.InputStream;

import org.mockito.Mockito;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.BeforeClass;



@Test
public class DomainModelHelperTest {
	
	
	
	private DomainModelHelper dh;

  @BeforeMethod
  public void beforeMethod() {
  }


  
  @BeforeClass
  public void beforeClass() {
	  dh = new DomainModelHelper();
	 
	 
  }


  @Test
  public void DomainModelHelper() {
   
  }

  @Test
  public void getCommonSchema() {
   
  }

  @Test
  public void getDomainSchema() {
    
  }

  @Test
  public void getkafkaTopic() {
   
  }

  
  @Test
  public void loadUnifiedSchema() {
	 
  }

  @Test
  public void loadtenantDomainSchemas() {
  
  }
}
