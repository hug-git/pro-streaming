package com.hug.canal.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Properties;

public class MyKafkaSender {

    private static KafkaProducer<String, String> kafkaProducer = null;

    private static KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        try {
            properties.load(new InputStreamReader(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties")),
                    StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }

        KafkaProducer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return producer;
    }

    public static void send(String topic, String msg) {
        if (kafkaProducer == null) {
            kafkaProducer = createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<>(topic, msg));
    }
}
