package com.example;

import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

import dbz.appdb.likes.Envelope;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.Trigger;

public class LikeCountSpark {
    private static final String BOOTSTRAP = "kafka:9092";
    private static final String TOPIC = "dbz.appdb.likes";

    public static void main(String[] args) throws Exception {
        var spark = SparkSession.builder()
            .appName("like-count-spark")
            .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        var events = spark.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "latest")
            .load()
            .filter(col("value").isNotNull())
            .select(from_avro(
                expr("substring(value, 6, length(value) - 5)"),
                Envelope.getClassSchema().toString()).alias("envelope"));

        var counts = events
            .select(
                coalesce(col("envelope.after.post_id"), col("envelope.before.post_id")).alias("post_id"),
                col("envelope.op").alias("op"))
            .groupBy(col("post_id"))
            .agg(sum(
                when(col("op").equalTo("d"), -1L)
                    .when(col("op").equalTo("u"), 0L)
                    .otherwise(1L)).alias("like_count"));

        var writer = counts.writeStream()
            .format("console")
            .outputMode(OutputMode.Complete())
            .option("truncate", false);

        if (args.length > 0) {
            writer = writer.trigger(Trigger.ProcessingTime(args[0] + " seconds"));
        }

        writer.start().awaitTermination();
    }
}
