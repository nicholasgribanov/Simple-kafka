package name.nicholasgribanov.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer1 {
    private static final Logger log = LoggerFactory.getLogger(Producer1.class);

    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            String key = "id_" +i;

            String topic = "first_topic";
            String value = "Hello, World! " + i;
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("Successfully sending message!");
                        log.info("\nTopic: " + recordMetadata.topic() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n");
                    } else {
                        log.error("Something wrong with sending: " + e);
                    }
                }
            });
        }

        producer.close();

    }
}
