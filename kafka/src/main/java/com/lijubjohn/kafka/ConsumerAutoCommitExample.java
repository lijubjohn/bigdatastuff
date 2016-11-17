package com.lijubjohn.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by Liju on 11/16/2016.
 */
public class ConsumerAutoCommitExample {

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = null;
        try {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", "test");
            //enable auto commit
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            // create kafka consumer
            consumer = new KafkaConsumer<String, String>(props);
            //add list of topics to be subscribed
            consumer.subscribe(Arrays.asList("test"));

            while (true) {
                // start polling records
                final ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
                for (ConsumerRecord record : consumerRecords) {
                    System.out.println(String.format("offset %d, partition %d ,key %s ,value %s",
                            record.offset(), record.partition(), record.key(), record.value()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("closing consumer");
            if (consumer!=null)
                consumer.close();
        }
    }
}
