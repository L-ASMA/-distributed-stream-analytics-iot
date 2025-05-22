package com.smartcity;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Random;

public class ElectricityKafkaProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("linger.ms", 5);
        props.put("compression.type", "snappy");

        Random rand = new Random();
        String[] zones = {"zone1", "zone2", "zone3", "zone4", "zone5"};

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                String zone = zones[rand.nextInt(zones.length)];

                double voltage;
                // 10% chance of anomaly
                if (rand.nextInt(10) == 0) {
                    // simulate spike: either very low or very high
                    if (rand.nextBoolean()) {
                        voltage = 140 + rand.nextDouble() * 20; // very low voltage: 140–160V
                    } else {
                        voltage = 260 + rand.nextDouble() * 20; // very high voltage: 260–280V
                    }
                } else {
                    voltage = 180 + rand.nextDouble() * 60; // normal voltage: 180–240V
                }

                long timestamp = System.currentTimeMillis();
                String message = zone + "," + voltage + "," + timestamp;

                producer.send(new ProducerRecord<>("topic_electricity", message), (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Message envoyé (électricité) : " + message);
                    } else {
                        exception.printStackTrace();
                    }
                });

                Thread.sleep(5000); // wait 5 seconds
            }
        }
    }
}
