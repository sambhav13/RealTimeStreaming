package goliath.analytics.process;

import goliath.analytics.util.FlumeDomainModelObservation;


import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.BeforeClass;
import org.testng.asserts.Assertion;

public class EventInterceptorTest {

	private EventInterceptor eveIntcptor ;
	private FlumeDomainModelObservation fdm = null;

	@Mock 
	Event ev;

	//@Mock
	List<Event> events;

	@Mock
	static Context ctx; 


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

		MockitoAnnotations.initMocks(this);
		eveIntcptor = new EventInterceptor(); 

		EventInterceptor.Builder builder = new EventInterceptor.Builder();
		builder.configure(ctx);

		Assert.assertNotNull(builder.build());


		fdm = new FlumeDomainModelObservation();
		fdm.configureProperties();
		fdm.loadUnifiedSchema();

		events = new ArrayList<Event>();
	}


	@Test(dependsOnGroups = { "listIntercept" })
	public void close() {
		eveIntcptor.close();
	}

	@Test(groups = "init")
	public void initialize() {
		eveIntcptor.initialize();
	}

	 @DataProvider(name="test1")
	  public Object[][] testData() {
		 		  
	    return new Object[][] {
	      new Object[] { "KA03HU8885,Sonia,WalmartTruckKA03HU8885,50.0,50.0,50.0,50.0,50.0,2015-02-16T15:55:26.910Z,1419139186871"}};
	    
	  }
	
	@Test(dependsOnGroups = "init",dataProvider = "test1")
	public void interceptEvent(String parsedData) {

		
		ev = EventBuilder.withBody(parsedData, Charset.forName("UTF-8"));
		eveIntcptor.intercept(ev);
	}


	@Test(groups = { "listIntercept" }, dependsOnGroups = { "init" },dataProvider = "test1")
	public void interceptListEvent(String parsedData) {

		Event tempEvent = EventBuilder.withBody(parsedData, Charset.forName("UTF-8"));
		
		Event nullEvent = null ;
		events.add(tempEvent);	 
		events.add(nullEvent);
		eveIntcptor.intercept(events);
	}


}
