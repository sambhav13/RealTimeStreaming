package goliath.analytics.sparkengine.exception;

/**
 * Custom SparkStreaming Exception class
 * @author GlobalLogic
 * 
 **/
public class SparkStreamingException extends Exception {
	private static final long serialVersionUID = -6397240408002644019L;

	/**
	 * Constructor
	 * @param msg
	 */
	public SparkStreamingException(String msg) {
		super(msg);
	}
}
