package com.example;

import com.datastax.oss.driver.api.core.CqlSession;
import java.net.InetSocketAddress;

public class LikeCassandra {
    public static void main(String[] args) {
        try (var session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("cassandra", 9042))
                .withLocalDatacenter("datacenter1")
                .build()) {

            session.execute(
                "CREATE KEYSPACE IF NOT EXISTS likes_ks "
                    + "WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
            session.execute(
                "CREATE TABLE IF NOT EXISTS likes_ks.likes "
                    + "(post_id text, user_id text, PRIMARY KEY (post_id, user_id))");

            var insert = session.prepare(
                "INSERT INTO likes_ks.likes (post_id, user_id) VALUES (?, ?)");
            session.execute(insert.bind("post-1", "user-7"));
            session.execute(insert.bind("post-1", "user-3"));
            session.execute(insert.bind("post-2", "user-5"));

            System.out.println("=== 全件 ===");
            for (var row : session.execute("SELECT post_id, user_id FROM likes_ks.likes")) {
                System.out.println(row.getString("post_id") + " <- " + row.getString("user_id"));
            }

            System.out.println("=== post-1 のいいね ===");
            for (var row : session.execute("SELECT user_id FROM likes_ks.likes WHERE post_id = 'post-1'")) {
                System.out.println(row.getString("user_id"));
            }
        }
    }
}
