package com.rrajesh.kafka.basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    final static String bootstrapServers = "localhost:9092";
    final static String topicName = "payment-ingestion";

    public static void main(String[] args) {
        //Create Producer Properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        //Create Producer Record
        ProducerRecord<String, String> message = new ProducerRecord<>(topicName, "Hello Kafka!!");

        //Send Data
        producer.send(message);
        producer.flush();
        producer.close();
    }
}
