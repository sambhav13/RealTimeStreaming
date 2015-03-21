//CustomInterceptor.java
//package goliath.analytics.flume;
package goliath.analytics.process;

import goliath.analytics.logging.GoliathLogger;

import java.util.Iterator;
import java.util.List;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Intercepts the Flume Events
 * @author sambhav.gupta
 *
 */
public class EventInterceptor implements Interceptor {

	private static final Logger log = LoggerFactory.getLogger(EventInterceptor.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flume.interceptor.Interceptor#close()
	 */
	@Override
	public void close() {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.flume.interceptor.Interceptor#initialize()
	 */
	@Override
	public void initialize() {

	}


	/*
	 * @see org.apache.flume.interceptor.Interceptor#intercept(org.apache.flume.Event)
	 *  
	 */
	@Override
	public Event intercept(Event event) {
		String methodName = "intercept";
		byte[] eventBody =null;
		if(event!=null)
		{
			eventBody = event.getBody();
		
		System.out.println("The Domain Model Interceptor output is\n" + new String(eventBody));
		GoliathLogger.info(log, methodName, "The Domain Model Interceptor output is\n" + new String(eventBody));
		}
		else{
			GoliathLogger.info(log, methodName, 
					"The Domain Model Interceptor output is\n" + new String("null...."));
		}
		return event;
	}

	/*
	 *  
	 * @see org.apache.flume.interceptor.Interceptor#intercept(java.util.List)
	 */
	@Override
	public List<Event> intercept(List<Event> events) {
		for (Iterator<Event> iterator = events.iterator(); iterator.hasNext();) {
			Event next = intercept(((Event) iterator.next()));
			if (next == null) {
				iterator.remove();
			}
		}
		return events;
	}

	/**
	 * Builder Class helps in setting up Flume Interceptor
	 * @author sambhav.gupta
	 * 
	 */
	public static class Builder implements Interceptor.Builder {
		@Override
		public void configure(Context context) {
			// TODO Auto-generated method stub
		}

		@Override
		public Interceptor build() {
			return new EventInterceptor();
		}
	}
}
