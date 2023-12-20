package com.yunus.kafkabasics;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemo {
    private static final Logger logger = LogManager.getLogger();
    public static void main(String[] args) {
        String groupId = "my-java-app";
        String topic = "demo_java";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        logger.info("properties created");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        logger.info("consumer created");

        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            logger.info("polling data");
            for (ConsumerRecord<String, String> record: records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }



    }
}
