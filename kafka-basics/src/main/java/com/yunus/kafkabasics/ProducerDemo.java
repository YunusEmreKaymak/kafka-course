package com.yunus.kafkabasics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;


public class ProducerDemo {
    private static final Logger logger = LogManager.getLogger();
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        logger.info("properties created");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        logger.info("producer created");
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java","hello world");
        logger.info("producerRecord created");
        producer.send(producerRecord);
        logger.info("producerRecord sent to the topic");
        producer.flush();
        producer.close();
        logger.info("producer closed");
    }
}
