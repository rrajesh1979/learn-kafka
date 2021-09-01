package com.rrajesh.kafka.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {
    final static String bootstrapServers = "localhost:9092";
    final static String topicName = "payment-ingestion";

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);

        //Create Producer Properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        for (int i=0; i < 10; i++) {
            String key = "key-" + i;
            String value = "Hello Kafka + !!" + i;
            //Create Producer Record
            ProducerRecord<String, String> message = new ProducerRecord<>(topicName, key, value);

            //Send Data
            producer.send(message, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //execute every time a messages is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info("Received new metadata \n"
                                + "Topic :{} \n"
                                + "Partition :{} \n"
                                + "Offset : {} \n"
                                + "Timestamp : {} \n",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp()
                        );
                    } else {
                        logger.error("Error while producing message : {}", e.getMessage());
                    }
                }
            });
        }

        producer.flush();
        producer.close();
    }
}
