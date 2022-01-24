package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        // create a logger for my class
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers = "127.0.0.1:9092";
        // create producer properties
        Properties properties = new Properties(); // create a new properties object
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // tell producer type of data sent, and how it will be serialized into bytes
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // kafka client convert the data sent to bytes, 0 and 1s

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

//        org.apache.kafka.clients.producer.internals.StickyPartitionCache is always used, if key wasn't specified.
//        (cache for  topic_name and topic_partition).
//        So now if key wasn't specified, client is always send to one random partition.


        for (int i=0; i<10; i++){
            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello world " + Integer.toString(i));
            // send data - asynchronous
            producer.send(record, new Callback() {
                @Override
                // onCompletion execute everytime there's
                // a record successfully sent
                // an exception is thrown
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) { // if error is null, success
                        logger.info("Received metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());

                    } else { // else, error
                        logger.error("Error while producing", e);

                    }

                }
            });
        }
        // flush data
        producer.flush(); // force all data to be produced
        // flush and close producer
        producer.close();
    }
}
