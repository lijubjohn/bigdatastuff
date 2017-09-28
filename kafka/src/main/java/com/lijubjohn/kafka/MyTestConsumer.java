package com.lijubjohn.kafka;

/**
 * Created by liju on 9/28/17.
 */
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liju on 9/27/17.
 */
public class MyTestConsumer {

    private static Logger logger = LoggerFactory.getLogger(MyTestConsumer.class);

    public static void main(String[] args) throws InterruptedException {

        int n = 2;
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
        for (int i = 0; i < n; i++) {
            KafkaConsumer consumer = createConsumer();
            consumer.subscribe(Collections.singletonList("ckdatabaseservice"));
            executor.submit(new MyConsumerThread(consumer, i));
        }
        executor.awaitTermination(60, TimeUnit.SECONDS);

    }

    static class MyConsumerThread implements Runnable {
        private KafkaConsumer consumer;
        private int id;

        public MyConsumerThread(KafkaConsumer consumer, int id) {
            this.consumer = consumer;
            this.id = id;
        }

        @Override
        public void run() {
            try {
                long start = System.currentTimeMillis();
                while (true) {
                    logger.info("id-" + id + " before poll");
                    final ConsumerRecords records = consumer.poll(1000);
                    logger.info("id-" + id + " record fetched , size = " + records.count());
                    if (id == 1 && (System.currentTimeMillis() - start) > 40000) {
                        logger.info("id-" + id + " going for long sleep");
                        Thread.sleep(60100);
                    } else {
                        Thread.sleep(500);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static KafkaConsumer<byte[], byte[]> createConsumer() {
        // Include any unknown worker configs so consumer configs can be set globally on the worker
        // and through to the task
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-grp");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ireporterkafka01.vp.if1.apple.com:9092");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "8000");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");

        KafkaConsumer<byte[], byte[]> newConsumer;
        try {
            newConsumer = new KafkaConsumer<>(props);
        } catch (Throwable t) {
            throw new ConnectException("Failed to create consumer", t);
        }
        return newConsumer;
    }

}
