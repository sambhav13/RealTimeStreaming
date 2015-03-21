package goliath.analytics.constants;

/**
 * This class contains Application constant
 * @author ravi.chaudhary
 * 
 */
public class ApplicationConstants {

    public static final String JSON_CONTENT_TYPE = "application/json";

    public static final String BINARY_CONTENT_TYPE = "avro/binary";

    public static final String sparkAppName = "sparkStreamAppName";

    public static final String sparkMaster = "sparkStreamMaster";

    public static final String sparkStreamingBatchWindow = "sparkBatchWindow";

    public static final String sparkFlumeStreamingMaster = "sparkMaster";

    public static final String sparkFlumeStreamingPort = "sparkPort";

    public static final String propertyFileName = "application.properties";
    
    public static final String testPropertyFileName = "applicationTest.properties";

    public static final String modelPropertyFileName = "model.properties";

    public static final String truckSchemaFile = "truck.avsc";
    
    public static final String boilerSchemaFile = "boiler.avsc";

    public static final String relSchemaFile = "reliance_poc.avsc";

    public static final String sysAttSchemaFile = "systemAttributes.avsc";

    public static final String cntxtSchemaFile = "context.avsc";

    public static final String flumeInstanceIpAddress = "FlumeInstanceIp";

    public static final String flumeInstancePort = "FlumeInstancePort";

    public static final String ServiceDBInstanceIp = "PLATFORM_SERVICES_HOSTNAME";

    public static final String ServiceDBInstancePort = "PLATFORM_SERVICES_PORT";

    public static final String ServiceSecretKey = "secret_key";

    public static final String tenantName1 = "reliancepoc";

    //public static final String tenantName = "acmeenggworks";
    
    public static final String tenantName = "glomart";

    public static final String metaBrokerList_PropName = "metadata.broker.list";

    public static final String serializerClass_PropName = "serializer.class";

    public static final String kafka_BrokerList = "kafkaBrokerList";

    public static final String kafka_Encoder = "kafkaEncoder";
    
    public static final String FlumeEngine = "Flume";
    
    public static final String SparkEngine = "Spark";
    
    public static final String kafka_SerializerClass = "kafka.serializer.StringEncoder";

}
