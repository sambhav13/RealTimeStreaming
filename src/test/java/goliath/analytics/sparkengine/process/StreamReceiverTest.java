package goliath.analytics.sparkengine.process;

import java.io.Serializable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.BeforeClass;

@Test
@ContextConfiguration(locations = { "classpath:applicationContext.xml" })
public class StreamReceiverTest  extends AbstractTestNGSpringContextTests {
	
	
	@Autowired 
	StreamReceiver str;
	
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
  public void StreamReceiver() {
    
  }

  @Test
  public void configureProperties() {
   
  }

  @Test
  public void createSparkConf() {
    
  }

  @Test
  public void createStreamingContext() {
   
  }

  @Test
  public void main() {
    
  }

  @Test
  public void start() {
    str.start();
  }
}
