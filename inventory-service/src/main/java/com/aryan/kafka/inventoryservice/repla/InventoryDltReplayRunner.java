package com.aryan.kafka.inventoryservice.repla;

import com.aryan.kafka.avro.OrderCreatedEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = "app.dlt.replay.enabled",
   havingValue = "true")
public class InventoryDltReplayRunner implements CommandLineRunner {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String orderCreatedTopic;

    public InventoryDltReplayRunner(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${spring.kafka.bootstrap-servers}")
            String bootstrapServers,
            @Value("${spring.kafka.consumer.properties.schema.registry.url}")
            String schemaRegistryUrl,
            @Value("${app.kafka.topics.order-created}")
            String orderCreatedTopic) {

        this.kafkaTemplate = kafkaTemplate;
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.orderCreatedTopic = orderCreatedTopic;
    }
    @Override
    public void run(String... args) throws Exception {
        Map<String, Object> properties = createConsumerProperties();

        try (
                KafkaConsumer<String, byte[]> consumer =
                        new KafkaConsumer<>(properties);

                KafkaAvroDeserializer avroDeserializer =
                        createAvroDeserializer()
        )  {
        String dltTopic=orderCreatedTopic+"-dlt";
            TopicPartition dltPartition = new TopicPartition(dltTopic, 1);
            consumer.assign(Collections.singletonList(dltPartition));
            consumer.seekToBeginning(Collections.singletonList(dltPartition));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);

            while (System.nanoTime() < deadline) {

                ConsumerRecords<String, byte[]> records =
                        consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, byte[]> record : records) {
                    System.out.println(
                            "DLT record: key=" + record.key()
                                    + ", partition=" + record.partition()
                                    + ", offset=" + record.offset()
                    );
                    Object decodedEvent = avroDeserializer.deserialize(
                            record.topic(),
                            record.value()
                    );

                    System.out.println("Decoded DLT event: " + decodedEvent);
                    if (!(decodedEvent instanceof OrderCreatedEvent)) {
                        continue;
                    }

                    OrderCreatedEvent event = (OrderCreatedEvent) decodedEvent;

                    if (event.getOrderId() != 20L
                            || !"66e3a335-a631-4351-99e1-dd99bd99dc99"
                            .equals(event.getEventId())) {
                        continue;
                    }

                    System.out.println(
                            "Replay target found: orderId=" + event.getOrderId()
                                    + ", eventId=" + event.getEventId()
                    );
                    SendResult<String, Object> result =
                            kafkaTemplate.send(
                                    orderCreatedTopic,
                                    record.key(),
                                    event
                            ).get(30, TimeUnit.SECONDS);

                    System.out.println(
                            "DLT event replayed: orderId=" + event.getOrderId()
                                    + ", topic=" + result.getRecordMetadata().topic()
                                    + ", partition=" + result.getRecordMetadata().partition()
                                    + ", offset=" + result.getRecordMetadata().offset()
                    );

                    return;

                }
            }
            System.out.println(
                    "Replay target nahi mila within the scan time; no event replayed."
            );
        }
    }
    private KafkaAvroDeserializer createAvroDeserializer() {
        Map<String, Object> properties = new HashMap<>();

        properties.put("schema.registry.url", schemaRegistryUrl);
        properties.put("specific.avro.reader", true);

        KafkaAvroDeserializer deserializer =
                new KafkaAvroDeserializer();

        deserializer.configure(properties, false);

        return deserializer;
    }
    private Map<String, Object> createConsumerProperties() {

        Map<String, Object> properties = new HashMap<>();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class
        );

        properties.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );

        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "none"
        );

        return properties;
    }
}
