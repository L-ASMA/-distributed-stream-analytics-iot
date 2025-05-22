package com.smartcity;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AirKafkaConsumer {

    public static void main(String[] args) {
        // Kafka configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "air-quality-consumer-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("topic_airquality"));

        // PostgreSQL configuration
        String url = "jdbc:postgresql://localhost:5432/smartcity";
        String user = "admin";
        String password = "admin";

        // Updated insert query matching the new format
        String insertSQL = """
            INSERT INTO airquality (
                zone, pm25, pm10, co2, temperature, humidity, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """;

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            System.out.println("Air Quality Consumer started. Listening to topic_airquality...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    String[] parts = record.value().split(",");

                    if (parts.length == 7) {
                        try (PreparedStatement stmt = conn.prepareStatement(insertSQL)) {
                            stmt.setString(1, parts[0].trim()); // zone
                            stmt.setInt(2, Integer.parseInt(parts[1].trim())); // pm25
                            stmt.setInt(3, Integer.parseInt(parts[2].trim())); // pm10
                            stmt.setInt(4, Integer.parseInt(parts[3].trim())); // co2
                            stmt.setDouble(5, Double.parseDouble(parts[4].trim())); // temperature
                            stmt.setDouble(6, Double.parseDouble(parts[5].trim())); // humidity
                            stmt.setTimestamp(7, Timestamp.valueOf(parts[6].trim())); // timestamp

                            stmt.executeUpdate();
                            System.out.println("Inserted air quality record for " + parts[0]);
                        } catch (Exception e) {
                            System.err.println("Insert error: " + e.getMessage());
                        }
                    } else {
                        System.out.println("Malformed record: " + record.value());
                    }
                }
            }

        } catch (SQLException e) {
            System.err.println("Database connection error: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
}
