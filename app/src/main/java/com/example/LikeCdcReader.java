package com.example;

import dbz.appdb.likes.Envelope;
import dbz.appdb.likes.Key;
import dbz.appdb.likes.Value;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.checkerframework.checker.nullness.qual.NonNull;

public class LikeCdcReader {
    public static void main(String[] args) {
        var props = getProperties();

        var emptyPolls = 0;
        try (Consumer<Key, Envelope> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("dbz.appdb.likes"));
            while (emptyPolls < 5) {
                var records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;
                for (ConsumerRecord<Key, Envelope> record : records) {
                    var key = record.key();
                    var value = record.value();
                    if (value == null) {
                        System.out.println("tombstone : " + key.getPostId() + " / " + key.getUserId());
                        System.out.println("----");
                        continue;
                    }
                    Value row = value.getAfter() != null ? value.getAfter() : value.getBefore();
                    System.out.println("op       : " + value.getOp());
                    System.out.println("key      : " + key.getPostId() + " / " + key.getUserId());
                    System.out.println("reaction : " + (row == null ? "(none)" : row.getReaction()));
                    System.out.println("----");
                }
            }
        }
    }

    private static @NonNull Properties getProperties() {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "cdc-reader");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}
