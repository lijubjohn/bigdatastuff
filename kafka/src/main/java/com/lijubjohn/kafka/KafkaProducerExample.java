package com.lijubjohn.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * Created by Liju on 11/15/2016.
 * Kafka producer example
 */
public class KafkaProducerExample {
    private final Producer<String, String> producer;

    KafkaProducerExample() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);
    }

    public void sendWithoutCallback(String topic, String key, String value) {

        producer.send(new ProducerRecord(topic, key, value));
    }

    public void sendWithCallback(String topic, String key, String value) {
        producer.send(new ProducerRecord<String, String>(topic, key, value), new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.println(String.format("Checksum %s ,offset %d ,partition %d ,timestamp %d", metadata.checksum(), metadata.offset(), metadata.partition(), metadata.timestamp()));
                } else {
                    exception.printStackTrace();
                }
            }
        });
    }

    public void close() {
        producer.close();
    }


    public static void main(String[] args) {
        KafkaProducerExample kafkaProducerExample = null;
        try {
            kafkaProducerExample = new KafkaProducerExample();
            for (int i = 0; i < 100; i++) {
                kafkaProducerExample.sendWithoutCallback("test", Integer.toString(i), Integer.toString(i));
            }

            for (int i = 0; i < 100; i++) {
                kafkaProducerExample.sendWithCallback("test", Integer.toString(i), Integer.toString(i));
            }
            //give sometime for callback
            Thread.sleep(2000);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            kafkaProducerExample.close();
        }
    }
}
