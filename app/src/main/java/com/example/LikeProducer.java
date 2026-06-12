package com.example;

import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class LikeProducer {
    private static final String[] posts = {"post-1", "post-2", "post-3"};

    public static void main(String[] args) throws InterruptedException {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        var random = new Random();

        try (var producer = new KafkaProducer<String, String>(props)) {
            for (var i = 0; i < 30; i++) {
                var postId = posts[random.nextInt(posts.length)];
                var userId = "user-" + (random.nextInt(20) + 1);
                producer.send(new ProducerRecord<>("likes", postId, userId));
                System.out.println("送信: " + userId + " が " + postId + " に ♥");
                Thread.sleep(300);
            }
            producer.flush();
        }
        System.out.println("送信完了");
    }
}
