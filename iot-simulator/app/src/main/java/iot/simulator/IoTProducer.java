package iot.simulator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class IoTProducer {
    private static final String TOPIC = "iot-sensor-data";

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();

        String[] deviceIds = {"device-1", "device-2", "device-3"};

        while (true) {
            String deviceId = deviceIds[random.nextInt(deviceIds.length)];
            double temperature = 15 + random.nextDouble() * 20;
            double humidity = 30 + random.nextDouble() * 40;
            long timestamp = System.currentTimeMillis();

            String event = String.format(
                    "{\"deviceId\":\"%s\",\"temperature\":%.2f,\"humidity\":%.2f,\"timestamp\":%d}",
                    deviceId, temperature, humidity, timestamp
            );

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, deviceId, event);
            RecordMetadata metadata = producer.send(record).get();

            System.out.printf("Sent: %s to partition %d with offset %d%n",
                    event, metadata.partition(), metadata.offset());

            Thread.sleep(1000);
        }
    }
}
