package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        System.out.println("hello world!");
        String bootstrapServers = "127.0.0.1:9092";
        //String groupId = "my-seven-application";
        String topic = "first_topic";
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
        // create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // compile from bytes to string
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// earliest/latest/none

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);// since we are going to read strings

        // Assign and seek are mostly used to replay data
        // or fetch a specific message

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15l;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // seek
        consumer.seek(partitionToReadFrom,offsetToReadFrom);
        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numOfMessagesReadSoFar = 0;

        // poll new data
        while(keepOnReading){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // pass a duration
            for (ConsumerRecord<String, String> record : records){
                numOfMessagesReadSoFar += 1;
                logger.info("Key: " + record.key() + "Value: " + record.value());
                logger.info("partition" + record.partition() + "offset: " + record.offset());
                if (numOfMessagesReadSoFar >= numberOfMessagesToRead){
                    keepOnReading = false; // exit the while loop
                    break;                 // exit the for loop
                }

            }


        }
        logger.info("Exiting the application");

    }
}
