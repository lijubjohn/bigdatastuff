package com.lijubjohn.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Created by Liju on 11/16/2016.
 */
public class ConsumerManualOffsetCommit {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = null;
        try {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", "test");
            //disable auto commit
            props.put("enable.auto.commit", "false");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            // create kafka consumer
            consumer = new KafkaConsumer(props);
            //add list of topics to be subscribed
            consumer.subscribe(Arrays.asList("test"));

            final int limit = 100;
            final List<ConsumerRecord<String,String>> buffer = new ArrayList();
            while (true) {
                // start polling records
                final ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
                for (ConsumerRecord record : consumerRecords) {
                    buffer.add(record);
                }
                //process fetched records
                if (buffer.size() >= limit){
                    processRecords(buffer);
                }

                //commit offsets
                consumer.commitAsync((offsets, exception) -> {
                    if (exception==null){
                        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()){
                            System.out.println(String.format("topic partition info %s , OffsetMetadata info %s ",
                                    entry.toString(),entry.getValue().toString()));
                        }
                    }else {
                        exception.printStackTrace();
                    }
                });

                //clear buffer
                buffer.clear();

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("closing consumer");
            if (consumer!=null)
                consumer.close();
        }
    }

    public static void processRecords(List<ConsumerRecord<String,String>> records){
        System.out.println("processing next batch of records");
        records.stream().forEach(record  ->
                System.out.println(String.format("offset %d, partition %d ,key %s ,value %s",
                record.offset(), record.partition(), record.key(), record.value())));
    }
}
