package goliath.analytics.protocol;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;

public class MessageDecoderTest {
	
	private MessageDecoder obj = null;
	byte[] b = null;
	@DataProvider
	public Object[][] avroSerializeInput(){
		Schema a = null;
		GenericRecord record = null;
		try {
		Schema.Parser parser = new Schema.Parser();
		File contextSchemaString = new File("src/test/resources/context.avsc");
		
			a = parser.parse(contextSchemaString);
		
		GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(a) ;
		record = genericRecordBuilder.build();
		System.out.println(record);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new Object[][] {
				new Object[] {record, a}
		};
	}
	
	@DataProvider
	public Object[][] avroDeserializeInput(){
		Schema a = null;
		GenericRecord record = null;
		try {
		Schema.Parser parser = new Schema.Parser();
		File contextSchemaString = new File("src/test/resources/context.avsc");
		a = parser.parse(contextSchemaString);
		

		GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(a) ;
		record = genericRecordBuilder.build();
		System.out.println(record);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new Object[][] {
				new Object[] {b, a ,record}
		};
	}
	
	
  @BeforeClass
  public void beforeClass() {
	  obj = new MessageDecoder();
  }

  @BeforeTest
  public void beforeTest() {
  }


  @Test(dataProvider = "avroDeserializeInput",dependsOnMethods = { "avroSerialize" })
  public void avroDeserialize(byte[] avroBytes, Schema schema,GenericRecord exp) {
	  GenericRecord rec = MessageDecoder.avroDeserialize(avroBytes, schema);
	  Assert.assertEquals(rec, exp);
	  
   
  }

  @Test(dataProvider = "avroSerializeInput")
  public void avroSerialize(GenericRecord serializableObject, Schema schema) {
     b = obj.avroSerialize(serializableObject, schema);
    GenericRecord actual = MessageDecoder.avroDeserialize(b, schema);
   Assert.assertEquals(serializableObject,actual);
  }

  @Test(dataProvider = "avroDeserializeInput")
  public void decodeMsg(byte[] avroBytes, Schema schema,GenericRecord exp) {
    GenericRecord actual = obj.decodeMsg(avroBytes, schema);
    Assert.assertEquals(actual, exp);
  }
}
