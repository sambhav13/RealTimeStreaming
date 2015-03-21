package goliath.analytics.process;



import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;




public class AnalyticsDataSinkTest {



	//@Mock
	//Channel ch;
	@Mock
	Context ctx;

	//@Mock
	private AnalyticsDataSink anSinkObj;


	@BeforeClass
	public void beforeClass() {

		MockitoAnnotations.initMocks(this);
		anSinkObj = new AnalyticsDataSink();
		anSinkObj.configure(ctx);
		anSinkObj.start();
	}


	@Test(dataProvider = "dp")
	public void f(Integer n, String s) {
	}


	@BeforeMethod
	public void initMocks(){
		
		
	}

	//private AnalyticsDataSink anSinkObj;

	@DataProvider
	public Object[][] dp() {
		return new Object[][] {
				new Object[] { 1, "a" },
				new Object[] { 2, "b" },
		};
	}


	@Test
	public void configure() {
		//
	}



	@Test
	public void process() {

		
		
		try {
			Sink.Status statusobj = anSinkObj.process();
			Assert.assertEquals(statusobj,Sink.Status.READY);
		} catch (EventDeliveryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Test
	public void start() {

		
	}

	@Test
	public void stop() {
		anSinkObj.stop();
	}
}
