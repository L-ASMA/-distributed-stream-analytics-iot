package com.smartcity;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class CrimeKafkaConsumer {

    public static void main(String[] args) {
        // Kafka Consumer config
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "crime-consumer-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("topic_crimes"));

        // PostgreSQL
        String url = "jdbc:postgresql://localhost:5432/smartcity";
        String user = "admin";
        String password = "admin";

        String insertSQL = """
                INSERT INTO crimes (
                    case_number, date, block, iucr, primary_type, description,
                    location_description, arrest, domestic, beat, district,
                    ward, community_area, fbi_code, x_coordinate, y_coordinate,
                    year, latitude, longitude
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        try (
            Connection conn = DriverManager.getConnection(url, user, password)
        ) {
            System.out.println("Consumer started. Listening to topic_crimes...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    String[] parts = record.value().split(",");

                    if (parts.length >= 19) {
                        try (PreparedStatement stmt = conn.prepareStatement(insertSQL)) {
                            stmt.setString(1, parts[1]);  // case_number
                            stmt.setString(2, parts[2]);  // date
                            stmt.setString(3, parts[3]);  // block
                            stmt.setString(4, parts[4]);  // iucr
                            stmt.setString(5, parts[5]);  // primary_type
                            stmt.setString(6, parts[6]);  // description
                            stmt.setString(7, parts[7]);  // location_description
                            stmt.setBoolean(8, Boolean.parseBoolean(parts[8]));   // arrest
                            stmt.setBoolean(9, Boolean.parseBoolean(parts[9]));   // domestic
                            stmt.setInt(10, Integer.parseInt(parts[10]));         // beat
                            stmt.setInt(11, Integer.parseInt(parts[11]));         // district
                            stmt.setInt(12, Integer.parseInt(parts[12]));         // ward
                            stmt.setInt(13, Integer.parseInt(parts[13]));         // community_area
                            stmt.setString(14, parts[14]);                        // fbi_code
                            stmt.setLong(15, Long.parseLong(parts[15]));          // x_coordinate
                            stmt.setLong(16, Long.parseLong(parts[16]));          // y_coordinate
                            stmt.setInt(17, Integer.parseInt(parts[17]));         // year
                            stmt.setDouble(18, Double.parseDouble(parts[19]));     // latitude
                            stmt.setDouble(19, Double.parseDouble(parts[20]));     // longitude

                            stmt.executeUpdate();
                            System.out.println("Inserted crime: " + parts[1]);
                        } catch (Exception e) {
                            System.err.println("Failed to insert record: " + e.getMessage());
                        }
                    } else {
                        System.out.println("Malformed record: " + record.value());
                    }
                }
            }

        } catch (SQLException e) {
            System.err.println("DB error: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
}
