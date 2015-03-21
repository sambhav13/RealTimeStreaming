package goliath.analytics.process;

import goliath.analytics.util.FlumeDomainModelObservation;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BuilderTest implements Interceptor.Builder{


	@Mock
	static Context ctx; 

	@Test(dataProvider = "dp")
	public void f(Integer n, String s) {
	}
	@BeforeMethod
	public  void beforeMethod() {


	}


	@DataProvider
	public  Object[][] dp() {
		return new Object[][] {
				new Object[] { 1, "a" },
				new Object[] { 2, "b" },
		};
	}
	@BeforeClass
	public  void beforeClass() {

		MockitoAnnotations.initMocks(this);

	}


	@Test
	public  void configure() {
		EventInterceptor.Builder builder = new EventInterceptor.Builder();
		builder.configure(ctx);
	}

	@Test
	public Interceptor build() {

		return new EventInterceptor();
	}
	@Override
	public void configure(Context arg0) {
		// TODO Auto-generated method stub
		
	}


}
