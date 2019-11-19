import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Random;
import org.apache.avro.Schema.Parser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.errors.ConnectException;

public class TestProducer {

  private static KafkaProducer createProducer1() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer1");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put("schema.registry.url", "http://localhost:8081");

    try {
      return new KafkaProducer<String, GenericRecord>(props);
    } catch (Throwable t) {
      throw new ConnectException("Failed to create producer", t);
    }
  }

//  private static KafkaProducer createProducer2() {
//    Properties props = new Properties();
//    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//    props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer2");
//    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
//
//    try {
//      return new KafkaProducer<String, GenericRecord>(props);
//    } catch (Throwable t) {
//      throw new ConnectException("Failed to create producer", t);
//    }
//  }

  private static Schema createSchema() {
    String schemaDef = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":["
        + "{\"name\":\"timestamp\",\"type\":{\"type\":\"long\", \"logicalType\":\"timestamp-millis\"}},"
        + "{\"name\":\"time\",\"type\":{\"type\":\"int\", \"logicalType\":\"time-millis\"}},"
        + "{\"name\":\"date\",\"type\":{\"type\":\"int\", \"logicalType\":\"date\"}},"
        + "{\"name\":\"decimal\",\"type\":{\"type\":\"bytes\", \"logicalType\":\"decimal\", \"precision\": 16, \"scale\": 5}}"
        + "]}";
    Schema.Parser parser = new Schema.Parser();
    return parser.parse(schemaDef);
  }

  private static Schema readInSchema() {
    File file = new File("/Users/jinxin.liu/schema.json");
    Schema.Parser parser = new Schema.Parser();
    try {
      return parser.parse(file);
    } catch (IOException e) {
      System.err.println("file not found");
      return null;
    }
  }

  private static ByteBuffer randDecimal(int precision) {
    Random rand = new Random();
    return ByteBuffer.wrap(new BigInteger(precision, rand).toByteArray());
  }

  private static GenericRecord prepareRecord(Schema schema) {
    GenericRecord avroRecord = new GenericData.Record(schema);
    Random rand = new Random();
    avroRecord.put("timestamp", (long) rand.nextInt(20000));
    avroRecord.put("time", rand.nextInt(20000));
    avroRecord.put("date", rand.nextInt(20000));
    avroRecord.put("decimal", randDecimal(16));

    return avroRecord;
  }

  public static void main(String[] args) {
    System.out.println("*** Starting Test Producer ***");
    final KafkaProducer<String, GenericRecord> producer1 = createProducer1();
    //final KafkaProducer<String, byte[]> producer2 = createProducer2();
//    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//      System.out.println("### Stopping Test Producer ###");
//      producer1.flush();
//      producer1.close();
//    }));


    final String key = "key1";
    final Schema schema = createSchema();
    for (int i = 0; i < 10; i++) {
      //GenericRecord avroRecord = new GenericData.Record(schema);
      //avroRecord.put("timestamp", 2000L);
      ProducerRecord<String, GenericRecord> record;
//      if (i % 2 == 0) {
//        record = new ProducerRecord<>("topic1", key, avroRecord);
//      } else {
//        record = new ProducerRecord<>("topic1", key, null);
//      }
      record = new ProducerRecord<>("topic1", key, prepareRecord(schema));
      producer1.send(record);
    }
    System.out.println("sent 10 messages");
    producer1.flush();
    producer1.close();

//    GenericRecord avroRecord1 = new GenericData.Record(schema);
//    avroRecord1.put("f1", "value1");
//    ProducerRecord<String, GenericRecord> record1 = new ProducerRecord<>("topic1", key, avroRecord1);
//    producer1.send(record1);
//
//    ProducerRecord<String, GenericRecord> record2 = new ProducerRecord<>("topic1", key, null);
//    producer1.send(record2);
//
//    GenericRecord avroRecord2 = new GenericData.Record(schema);
//    avroRecord2.put("f1", "remote test");
//    ProducerRecord<String, GenericRecord> record3 = new ProducerRecord<>("topic1", key, avroRecord2);
//    producer1.send(record3);
//
//    GenericRecord avroRecord3 = new GenericData.Record(schema);
//    avroRecord3.put("f1", null);
//    ProducerRecord<String, GenericRecord> record4 = new ProducerRecord<>("topic1", key, avroRecord3);
//    producer1.send(record4);

//    final Schema schema = readInSchema();
//    for (int i = 0; i < 6; i++) {
//      GenericRecord avroRecord = prepareRecord(schema);
//      ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(
//          "topic1",
//          key,
//          avroRecord);
//      producer1.send(record);
//    }
//    System.out.println("sent 6 messages");

//    byte[] bytes = new byte[20];
//    new Random().nextBytes(bytes);
//    ProducerRecord<String, byte[]> record2 = new ProducerRecord<>("topic1", key, bytes);
//    producer2.send(record2);

    //    avroRecord.put("last_update_time", rand.nextInt(20000));
////    avroRecord.put("day_avg_px", randDecimal(38));
////    avroRecord.put("limit_price", randDecimal(38));
////    avroRecord.put("pegg_offset_value", randDecimal(10));
////    avroRecord.put("capital_commitment_qty", randDecimal(18));
////    avroRecord.put("stock_ref_price", randDecimal(38));
////    avroRecord.put("auto_ioi_offset", randDecimal(28));
////    avroRecord.put("settl_date", rand.nextInt(20000));
////    avroRecord.put("trade_date", rand.nextInt(20000));

  }

}