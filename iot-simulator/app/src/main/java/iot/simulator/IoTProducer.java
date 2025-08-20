package iot.simulator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class IoTProducer {

    private static final String TOPIC = "iot-sensor-data";
    private static final Random random = new Random();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            SensorData data = new SensorData(
                    "device-" + (i % 3),
                    random.nextInt(50),   // temperature
                    random.nextInt(100)   // humidity
            );

            String json;
            try {
                json = mapper.writeValueAsString(data);
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                e.printStackTrace();
                continue;
            }

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC, data.deviceId, json);

            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("Sent record(key=%s value=%s) meta(partition=%d, offset=%d)%n",
                    record.key(), record.value(), metadata.partition(), metadata.offset());

            Thread.sleep(1000);
        }

        producer.close();
    }

    // Nested class for data model
    static class SensorData {
        public String deviceId;
        public int temperature;
        public int humidity;

        public SensorData(String deviceId, int temperature, int humidity) {
            this.deviceId = deviceId;
            this.temperature = temperature;
            this.humidity = humidity;
        }
    }
}
