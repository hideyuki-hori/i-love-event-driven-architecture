package com.example;

import dbz.appdb.likes.Envelope;
import dbz.appdb.likes.Key;
import dbz.appdb.likes.Value;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import static java.lang.Thread.sleep;

public class LikeCountStreams {
    static void main() throws Exception {
        var registryUrl = "http://schema-registry:8081";
        var serdeConfig = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);

        var keySerde = new SpecificAvroSerde<Key>();
        keySerde.configure(serdeConfig, true);
        var envSerde = new SpecificAvroSerde<Envelope>();
        envSerde.configure(serdeConfig, false);
        var rowSerde = new SpecificAvroSerde<Value>();
        rowSerde.configure(serdeConfig, false);

        var builder = new StreamsBuilder();
        KStream<Key, Envelope> source = builder.stream("dbz.appdb.likes", Consumed.with(keySerde, envSerde));

        source
            .groupBy((k, _) -> k.getPostId(), Grouped.with(Serdes.String(), envSerde))
            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("naive-count")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
            .toStream()
            .to("likes-count-naive", Produced.with(Serdes.String(), Serdes.Long()));

        KTable<Key, Value> current = source
            .groupByKey(Grouped.with(keySerde, envSerde))
            .aggregate(
                () -> null,
                (k, env, agg) -> env.getAfter(),
                Materialized.<Key, Value, KeyValueStore<Bytes, byte[]>>as("current-likes")
                    .withKeySerde(keySerde).withValueSerde(rowSerde));

        current
            .groupBy((pk, row) -> KeyValue.pair(
                row.getPostId(), row),
                Grouped.with(Serdes.String(), rowSerde))
            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count-by-post")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
            .toStream()
            .to("likes-count-by-post", Produced.with(Serdes.String(), Serdes.Long()));

        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "like-count");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        var streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();

        ReadOnlyKeyValueStore<String, Long> naive = null;
        ReadOnlyKeyValueStore<String, Long> correct = null;
        var deadline = System.currentTimeMillis() + 60000;
        while (System.currentTimeMillis() < deadline) {
            sleep(1000);
            if (streams.state() != KafkaStreams.State.RUNNING) {
                continue;
            }
            try {
                naive = streams.store(StoreQueryParameters.fromNameAndType("naive-count", QueryableStoreTypes.keyValueStore()));
                correct = streams.store(StoreQueryParameters.fromNameAndType("count-by-post", QueryableStoreTypes.keyValueStore()));
                if (hasEntries(correct)) {
                    break;
                }
            }  catch (Exception e) {
                // noop
            }
        }

        System.out.println("[naive] CDC イベントを数える");
        print(naive);
        System.out.println("[correct] table に畳んでから数える");
        print(correct);

        streams.close();
    }

    private static boolean hasEntries(ReadOnlyKeyValueStore<String, Long> store) {
        try (var it = store.all()) {
            return it.hasNext();
        }
    }

    private static void print(ReadOnlyKeyValueStore<String, Long> store) {
        if (store == null) {
            System.out.println("  (まだ集計できていません)");
            return;
        }
        try (var it = store.all()) {
            while (it.hasNext()) {
                var kv = it.next();
                System.out.println("  " + kv.key + " : " + kv.value);
            }
        }
    }
}
