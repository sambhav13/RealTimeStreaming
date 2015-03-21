package goliath.analytics.sparkengine.parsers;

import java.io.Serializable;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

public class SparkFlumeEvents  implements Serializable {

	private SparkFlumeEvent sparkEventName;

	public SparkFlumeEvent getSparkEventName() {
		return sparkEventName;
	}

	public void setSparkEventName(SparkFlumeEvent sparkEventName) {
		this.sparkEventName = sparkEventName;
	}
	

}
