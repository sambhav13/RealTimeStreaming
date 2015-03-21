package goliath.analytics.protocol;

import goliath.analytics.errorHandling.GoliathBaseException;
import goliath.analytics.logging.GoliathLogger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Message Decoder to decode Avro Messages
 * @author sambhav.gupta
 *
 */
@Component
public class MessageDecoder {

	private static final Logger log = LoggerFactory.getLogger(MessageDecoder.class);
    
	/**
	 *  Constructor
	 */
	public MessageDecoder() {
        String methodName = "MessageDecoder";
        GoliathLogger.start(log, methodName);

        System.out.println("Message Decoder Initialized...!!");
        log.info("Message Decoder Initialized...!!");
        GoliathLogger.info(log, methodName, "Message Decoder Initialized...!!");
        GoliathLogger.end(log, methodName);
    }

    /**
     * deserializes the avro Encoded data
     * @param avroBytes
     * @param schema
     * @return Genericrecord
     */
    public static GenericRecord avroDeserialize(byte[] avroBytes, Schema schema) {

        String methodName = "avroDeserialize";
        GoliathLogger.start(log, methodName);

        GenericRecord ret = null;
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(avroBytes);
            Decoder dec = DecoderFactory.get().directBinaryDecoder(in, null);
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
            ret = reader.read(null, dec);
        }
        catch (IOException ioe) {
            log.error(ioe.getMessage(), ioe);
            throw new GoliathBaseException(ioe.getMessage(), "avroDeserialize");
        }

        System.out.println("The Domain Observation is deserialized...!!");
        System.out.println("The raw parsed observation  is----->"+ret.toString());
		System.out.println("**************\n**************\n**************\n");
		
        log.info("The Domain Observation is deserialized...!!");
        GoliathLogger.info(log, methodName, "The Domain Observation is deserialized...!!");
        GoliathLogger.end(log, methodName);
        return ret;
    }
    
    
	/**
	 * 
	 * Serializes the avro Generic Record data
	 * @param serializableObject
	 * @param schema
	 * @return
	 */
	public  byte[] avroSerialize(GenericRecord serializableObject, Schema schema) {
	     byte[] avroBytes = null;
	    
	       ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
	       Encoder encoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
	       DatumWriter writer = new GenericDatumWriter(schema);
	       try {
			writer.write(serializableObject, encoder);
		
	       encoder.flush();
	       } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	       avroBytes = outputStream.toByteArray();
	     
	     return avroBytes;
	   }

    /**
     * Decodes the Avro Message
     * @param encodedData
     * @param commonSchema
     * @return returns the avro decoded message
     */
    public GenericRecord decodeMsg(byte[] encodedData, Schema commonSchema) {
        String methodName = "decodeMsg";
        GoliathLogger.start(log, methodName);

        GenericRecord decodedRec = avroDeserialize(encodedData, commonSchema);

        System.out.println("Message Decoder Initialized...!!");
        log.info("The Domain Message is Decoded...!!");
        GoliathLogger.info(log, methodName, "The Domain Message is Decoded...!!");
        GoliathLogger.end(log, methodName);
        return decodedRec;
    }
}
