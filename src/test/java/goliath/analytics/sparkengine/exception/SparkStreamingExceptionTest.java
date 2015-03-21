package goliath.analytics.sparkengine.exception;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.BeforeClass;

public class SparkStreamingExceptionTest {
	
	private SparkStreamingException sprstrExcep;
	
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
  public void SparkStreamingException() {
	  String msg ="futures timed out ";
	  sprstrExcep = new SparkStreamingException(msg);
	  
  }
}
