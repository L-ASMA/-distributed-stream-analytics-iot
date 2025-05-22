package com.smartcity;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class WaterKafkaConsumer {
    public static void main(String[] args) {
        // Kafka Consumer configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "water-consumer-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("topic_water"));

        // PostgreSQL connection configuration
        String jdbcUrl = "jdbc:postgresql://localhost:5432/smartcity";
        String user = "admin";
        String password = "admin";

        String insertSQL = "INSERT INTO water_flows (zone, flowrate, timestamp) VALUES (?, ?, ?)";

        System.out.println("Consumer started. Listening to topic_water...");

        try (
            Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
        ) {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    String[] parts = record.value().split(",");

                    if (parts.length == 3) {
                        String zone = parts[0];
                        double flowRate = Double.parseDouble(parts[1]);
                        long timestamp = Long.parseLong(parts[2]);

                        try (PreparedStatement stmt = conn.prepareStatement(insertSQL)) {
                            stmt.setString(1, zone);
                            stmt.setDouble(2, flowRate);
                            stmt.setLong(3, timestamp);
                            stmt.executeUpdate();
                            System.out.println("Inserted into PostgreSQL: " + record.value());
                        } catch (SQLException e) {
                            System.err.println("Insert failed: " + e.getMessage());
                        }
                    } else {
                        System.out.println("Malformed message: " + record.value());
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("Database connection failed: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
}
// package com.smartcity;

// import com.mongodb.client.*;
// import org.apache.kafka.clients.consumer.*;
// import org.apache.kafka.common.serialization.StringDeserializer;
// import org.bson.Document;

// import java.time.Duration;
// import java.util.Arrays;
// import java.util.Properties;

// public class WaterKafkaConsumer {
//     public static void main(String[] args) {
//         // Config Kafka Consumer
//         Properties props = new Properties();
//         props.put("bootstrap.servers", "localhost:9092");
//         props.put("group.id", "water-consumer-group");
//         props.put("key.deserializer", StringDeserializer.class.getName());
//         props.put("value.deserializer", StringDeserializer.class.getName());
//         props.put("auto.offset.reset", "earliest"); // read from beginning

//         KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//         consumer.subscribe(Arrays.asList("topic_water"));

//         // Config MongoDB client
//         MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
//         MongoDatabase database = mongoClient.getDatabase("smart-city-backend");
//         MongoCollection<Document> collection = database.getCollection("water_flows");

//         System.out.println("Consumer started. Listening to topic_water...");

//         try {
//             while (true) {
//                 ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

//                 for (ConsumerRecord<String, String> record : records) {
//                     String[] parts = record.value().split(",");

//                     if (parts.length == 3) {
//                         String zone = parts[0];
//                         double flowRate = Double.parseDouble(parts[1]);
//                         long timestamp = Long.parseLong(parts[2]);

//                         Document doc = new Document("zone", zone)
//                                 .append("flowRate", flowRate)
//                                 .append("timestamp", timestamp);

//                         collection.insertOne(doc);
//                         System.out.println("Inserted into MongoDB: " + doc.toJson());
//                     } else {
//                         System.out.println("Malformed message: " + record.value());
//                     }
//                 }
//             }
//         } finally {
//             consumer.close();
//             mongoClient.close();
//         }
//     }
// }


