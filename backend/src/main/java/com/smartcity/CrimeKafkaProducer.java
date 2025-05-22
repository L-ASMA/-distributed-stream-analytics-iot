
package com.smartcity;

import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;
import java.util.Random;

public class CrimeKafkaProducer {

    public static void main(String[] args) {
        String csvFilePath = "C:\\Users\\Hp\\OneDrive\\Bureau\\smart-city-backend\\Crime_Dataset.csv";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Random rand = new Random();

        try (
            Producer<String, String> producer = new KafkaProducer<>(props);
            BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))
        ) {
            String line;
            int count = 0;

            // Lire et stocker l’en-tête
            String header = reader.readLine();

            while ((line = reader.readLine()) != null) {
                String[] columns = line.split(",", -1);

                if (columns.length < 20) continue;

                try {
                    double latitude = Double.parseDouble(columns[headerIndex("Latitude", header)]);
                    double longitude = Double.parseDouble(columns[headerIndex("Longitude", header)]);

                    
                    latitude += rand.nextDouble() * 0.001 - 0.0005;
                    longitude += rand.nextDouble() * 0.001 - 0.0005;

                    columns[headerIndex("Latitude", header)] = String.format("%.6f", latitude);
                    columns[headerIndex("Longitude", header)] = String.format("%.6f", longitude);
                } catch (NumberFormatException e) {
                    continue;
                }

        
                String modifiedLine = String.join(",", columns);

                producer.send(new ProducerRecord<>("topic_crimes", modifiedLine), (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Sent: " + modifiedLine);
                    } else {
                        exception.printStackTrace();
                    }
                });

                Thread.sleep(500);
                count++;
            }

            System.out.println("CSV streaming terminé.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Fonction utilitaire pour obtenir l’index d’une colonne
    private static int headerIndex(String columnName, String headerLine) {
        String[] headers = headerLine.split(",");
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].trim().equalsIgnoreCase(columnName.trim())) {
                return i;
            }
        }
        return -1; // Not found
    }
}
