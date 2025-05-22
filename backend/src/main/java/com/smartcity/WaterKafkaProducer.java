
package com.smartcity;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.Random;
import java.util.Collections;
import java.util.List;
import java.util.Arrays;

public class WaterKafkaProducer {
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
        List<String> zones = Arrays.asList("zone1", "zone2", "zone3", "zone4", "zone5");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                Collections.shuffle(zones);  

                for (String zone : zones) {
                    double flowRate = 10 + rand.nextDouble() * 30;
                    long timestamp = System.currentTimeMillis();

                    String message = zone + "," + flowRate + "," + timestamp;

                    producer.send(new ProducerRecord<>("topic_water", message), (metadata, exception) -> {
                        if (exception == null) {
                            System.out.println("Message envoyé : " + message);
                        } else {
                            exception.printStackTrace();
                        }
                    });

                    Thread.sleep(500); 
                }

                Thread.sleep(1000); 
            }
        }
    }
}
