package com.example;

import java.sql.DriverManager;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class LikeLoadWriter {
    public static void main(String[] args) throws Exception {
        var perSecond = args.length > 0 ? Integer.parseInt(args[0]) : 100;
        var url = "jdbc:mysql://mysql:3306/appdb";
        try (var conn = DriverManager.getConnection(url, "dbz", "dbz");
                var insert = conn.prepareStatement(
                    "INSERT INTO likes (post_id, user_id, reaction) VALUES (?, ?, 'like')")) {
            var total = 0L;
            while (true) {
                var start = System.nanoTime();
                for (var i = 0; i < perSecond; i++) {
                    insert.setString(1, "post-" + ThreadLocalRandom.current().nextInt(1, 6));
                    insert.setString(2, UUID.randomUUID().toString());
                    insert.executeUpdate();
                }
                total += perSecond;
                var elapsedMs = (System.nanoTime() - start) / 1_000_000;
                System.out.println("INSERT: " + perSecond + "件 / " + elapsedMs + "ms (累計 " + total + "件)");
                if (elapsedMs < 1000) {
                    Thread.sleep(1000 - elapsedMs);
                }
            }
        }
    }
}
