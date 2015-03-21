package goliath.analytics.sparkengine.parsers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import goliath.analytics.util.FlumeDomainModelObservation;

import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.BeforeClass;


@Test
@ContextConfiguration(locations = { "classpath:applicationContext.xml" })
public class FlumeMessageParserTest extends AbstractTestNGSpringContextTests {



	@Autowired
	private FlumeMessageParser flumeMessageHandler ;

	private FlumeDomainModelObservation fdm = null;

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

		fdm = new FlumeDomainModelObservation();
		fdm.configureProperties();
		fdm.loadUnifiedSchema();
	}


	@SuppressWarnings("null")
	@Test(dependsOnGroups = "Flumeinit")
	public void call() {

		SparkFlumeEvent sparkFlumeEvent = null ;
		AvroFlumeEvent avrFlumEvent = new AvroFlumeEvent() ;
		String args[] = {"172.22.95.105","4141","KA03HU8885","Sonia","WalmartTruckKA03HU8885","50","2015-02-16T15:55:26.910Z"};
		byte b[] =fdm.createDomainObject(args);
		ByteBuffer buf = ByteBuffer.wrap(b);

		avrFlumEvent.setBody(buf);


		if(avrFlumEvent!=null)
		{
			sparkFlumeEvent = SparkFlumeEvent.fromAvroFlumeEvent(avrFlumEvent);
		}

		List<SparkFlumeEvent> eventList = new ArrayList<SparkFlumeEvent>();
		eventList.add(sparkFlumeEvent);

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("KafkaParser");
		sparkConf.setMaster("local[2]");
		
		//sparkConf.setMaster("dev05.goliath.globallogic.com");
	
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		JavaRDD<SparkFlumeEvent> sparkFlumeRdd = jsc.parallelize(eventList);

		try {
			flumeMessageHandler.call(sparkFlumeRdd);
		} catch (Exception e) {
		
			e.printStackTrace();
		}


	}

	@Test(dependsOnGroups = "Flumeinit")
	public void close() {
		flumeMessageHandler.close();
	}

	@Test(groups = "Flumeinit") 
	public void init()
	{
		flumeMessageHandler.init();
	}

}
