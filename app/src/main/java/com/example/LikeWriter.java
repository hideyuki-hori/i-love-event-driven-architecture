package com.example;

import java.sql.DriverManager;

public class LikeWriter {
    public static void main(String[] args) throws Exception {
        var url = "jdbc:mysql://mysql:3306/appdb";
        try (var conn = DriverManager.getConnection(url, "dbz", "dbz")) {
            try (var insert = conn.prepareStatement(
                    "INSERT INTO likes (post_id, user_id, reaction) VALUES (?, ?, ?)")) {
                insert.setString(1, "post-1");
                insert.setString(2, "user-7");
                insert.setString(3, "like");
                insert.executeUpdate();

                insert.setString(1, "post-1");
                insert.setString(2, "user-3");
                insert.setString(3, "like");
                insert.executeUpdate();

                insert.setString(1, "post-2");
                insert.setString(2, "user-5");
                insert.setString(3, "love");
                insert.executeUpdate();
            }
            System.out.println("INSERT: 3件");

            try (var update = conn.prepareStatement(
                    "UPDATE likes SET reaction = ? WHERE post_id = ? AND user_id = ?")) {
                update.setString(1, "love");
                update.setString(2, "post-1");
                update.setString(3, "user-7");
                update.executeUpdate();
            }
            System.out.println("UPDATE: post-1 / user-7 を love に");

            try (var delete = conn.prepareStatement(
                    "DELETE FROM likes WHERE post_id = ? AND user_id = ?")) {
                delete.setString(1, "post-2");
                delete.setString(2, "user-5");
                delete.executeUpdate();
            }
            System.out.println("DELETE: post-2 / user-5");
        }
    }
}
