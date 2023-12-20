package com.yunus.kafkabasics;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 30; i++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("second_topic", "hello world " + i);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            logger.info(
                                    "Received new metadata \n" +
                                            "Topic: " + recordMetadata.topic() + "\n" +
                                            "Partition: " + recordMetadata.partition() + "\n" +
                                            "Offset: " + recordMetadata.offset() + "\n" +
                                            "Timestamp: " + recordMetadata.timestamp()
                            );
                        } else {
                            logger.error("Error while producing", e);
                        }
                    }
                });

            }
            try {
                Thread.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        producer.flush();
        producer.close();
    }
}
