package com.example;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class LikeConsumer {
    public static void main(String[] args) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "like-aggregator");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var counts = new HashMap<String, Long>();
        var partitions = new HashMap<String, Integer>();

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("likes"));
            while (true) {
                var records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> record : records) {
                    counts.merge(record.key(), 1L, Long::sum);
                    partitions.put(record.key(), record.partition());
                }
                System.out.println("♥ 集計: " + new TreeMap<>(counts));
                System.out.println("振り分け: " + new TreeMap<>(partitions));
            }
        }
    }
}
