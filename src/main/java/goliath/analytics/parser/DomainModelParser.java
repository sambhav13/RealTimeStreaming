package goliath.analytics.parser;


import goliath.analytics.logging.GoliathLogger;
import goliath.analytics.protocol.MessageDecoder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


/**
 * Parses the Domain Model Observation
 * @author sambhav.gupta
 *
 */
@Component("modelParser")
public class DomainModelParser {

	@Autowired
    private MessageDecoder msgDecoder ;

    private static final Logger log = LoggerFactory.getLogger(DomainModelParser.class);

    /**
     * Constructor
     * 
     */
    public DomainModelParser()  {
        final String methodName = "DomainModelParser Constructor";
        GoliathLogger.start(log, methodName);

        log.info(methodName + ":" + "DomainModelParser  handles configured...");
        GoliathLogger.info(log, methodName, "DomainModelParser  handles configured...");
        GoliathLogger.end(log, methodName);
    }

  

    /**
     * This method parses given domain model observation using avro deserializer
     * and returns Avro specific record.
     * @param domainModelObservation
     * @return The parsedObservation using the Common Schema
     */
    public GenericRecord parseDomainModelObservation(byte[] domainModelObservation, Schema commonSchema) {
        final String methodName = "parseDomainModelObservation";
        GoliathLogger.start(log, methodName);

        GenericRecord observation = msgDecoder.decodeMsg(domainModelObservation, commonSchema);

        log.info(methodName + ":" + "Domain Object Parsed..");
        GoliathLogger.info(log, methodName, "Domain Object Parsed..");
        GoliathLogger.end(log, methodName);
        return observation;
    }

   


}
