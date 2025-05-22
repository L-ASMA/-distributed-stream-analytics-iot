package com.smartcity;

import org.apache.kafka.clients.producer.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;

public class AirKafkaProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String topic = "topic_airquality";
        Random random = new Random();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {

            System.out.println("Air quality producer started...");

            while (true) {
                String zone = "Zone-" + (random.nextInt(5) + 1);
                int pm25 = random.nextInt(100);
                int pm10 = random.nextInt(150);
                int co2 = 350 + random.nextInt(400);
                double temperature = 15 + (25 * random.nextDouble());
                double humidity = 20 + (60 * random.nextDouble());
                String timestamp = LocalDateTime.now().format(formatter);

                String generatedLine = String.format("%s,%d,%d,%d,%.2f,%.2f,%s",
                        zone, pm25, pm10, co2, temperature, humidity, timestamp);

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, generatedLine);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Sent: " + generatedLine);
                    } else {
                        exception.printStackTrace();
                    }
                });

                Thread.sleep(1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
