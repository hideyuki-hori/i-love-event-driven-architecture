package com.example;

import dbz.appdb.likes.Envelope;
import dbz.appdb.likes.Value;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

public class LikeCountFlink {
    private static final String BOOTSTRAP = "kafka:9092";
    private static final String REGISTRY = "http://schema-registry:8081";
    private static final String TOPIC = "dbz.appdb.likes";

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Envelope> source = KafkaSource.<Envelope>builder()
            .setBootstrapServers(BOOTSTRAP)
            .setTopics(TOPIC)
            .setGroupId("like-count-flink")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(
                ConfluentRegistryAvroDeserializationSchema.forSpecific(Envelope.class, REGISTRY))
            .build();

        DataStream<Envelope> events =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "likes-cdc");

        events
            .keyBy(LikeCountFlink::postId)
            .process(new NaiveCount())
            .sinkTo(countSink("likes-count-naive"))
            .name("naive-sink");

        events
            .keyBy(LikeCountFlink::primaryKey)
            .process(new RowDelta())
            .keyBy(delta -> delta.f0)
            .process(new SumDelta())
            .sinkTo(countSink("likes-count-by-post"))
            .name("correct-sink");

        env.execute("like-count-flink");
    }

    private static String postId(Envelope envelope) {
        Value row = "d".equals(envelope.getOp()) ? envelope.getBefore() : envelope.getAfter();
        return row.getPostId();
    }

    private static String primaryKey(Envelope envelope) {
        Value row = "d".equals(envelope.getOp()) ? envelope.getBefore() : envelope.getAfter();
        return row.getPostId() + "/" + row.getUserId();
    }

    private static KafkaSink<Tuple2<String, Long>> countSink(String topic) {
        return KafkaSink.<Tuple2<String, Long>>builder()
            .setBootstrapServers(BOOTSTRAP)
            .setRecordSerializer((element, context, timestamp) ->
                new ProducerRecord<>(
                    topic,
                    element.f0.getBytes(StandardCharsets.UTF_8),
                    String.valueOf(element.f1).getBytes(StandardCharsets.UTF_8)))
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    }

    public static class NaiveCount
            extends KeyedProcessFunction<String, Envelope, Tuple2<String, Long>> {
        private transient ValueState<Long> count;

        @Override
        public void open(OpenContext openContext) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("naive-count", Long.class));
        }

        @Override
        public void processElement(Envelope envelope, Context ctx, Collector<Tuple2<String, Long>> out)
                throws Exception {
            long current = (count.value() == null ? 0L : count.value()) + 1L;
            count.update(current);
            out.collect(Tuple2.of(ctx.getCurrentKey(), current));
        }
    }

    public static class RowDelta
            extends KeyedProcessFunction<String, Envelope, Tuple2<String, Long>> {
        private transient ValueState<Boolean> present;

        @Override
        public void open(OpenContext openContext) {
            present = getRuntimeContext().getState(new ValueStateDescriptor<>("present", Boolean.class));
        }

        @Override
        public void processElement(Envelope envelope, Context ctx, Collector<Tuple2<String, Long>> out)
                throws Exception {
            boolean deleted = "d".equals(envelope.getOp());
            Value row = deleted ? envelope.getBefore() : envelope.getAfter();
            if (row == null) {
                return;
            }
            boolean exists = Boolean.TRUE.equals(present.value());
            if (deleted) {
                if (exists) {
                    out.collect(Tuple2.of(row.getPostId(), -1L));
                    present.update(false);
                }
            } else if (!exists) {
                out.collect(Tuple2.of(row.getPostId(), 1L));
                present.update(true);
            }
        }
    }

    public static class SumDelta
            extends KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>> {
        private transient ValueState<Long> sum;

        @Override
        public void open(OpenContext openContext) {
            sum = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Long.class));
        }

        @Override
        public void processElement(Tuple2<String, Long> delta, Context ctx, Collector<Tuple2<String, Long>> out)
                throws Exception {
            long current = (sum.value() == null ? 0L : sum.value()) + delta.f1;
            sum.update(current);
            out.collect(Tuple2.of(delta.f0, current));
        }
    }
}
