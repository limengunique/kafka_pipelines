package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        //System.out.println("Hello world!");
        String bootstrapServers = "127.0.0.1:9092";
        // create producer properties
        Properties properties = new Properties(); // create a new properties object
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // tell producer type of data sent, and how it will be serialized into bytes
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // kafka client convert the data sent to bytes, 0 and 1s

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello world!");
        // send data - asynchrnous
        producer.send(record);
        // flush data
        producer.flush(); // force all data to be produced
        // flush and close producer
        producer.close();
    }
}
