package com.aryan.kafka.inventoryservice;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.springframework.test.context.ActiveProfiles;
import com.aryan.kafka.avro.OrderCreatedEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(
        partitions = 3,
        topics = {"orders.created.avro", "orders.created.avro-dlt"}
)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.group-id=inventory-dlt-integration-test",
        "spring.kafka.listener.auto-startup=true"
})
@ActiveProfiles("test")
class InventoryRetryToDltIntegrationTests {

    private static final String SOURCE_TOPIC = "orders.created.avro";
    private static final String DLT_TOPIC = "orders.created.avro-dlt";
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void failedInventoryEventIsPublishedToDltWithFailureMetadata() {
        Map<String, Object> consumerProperties =
                KafkaTestUtils.consumerProps("dlt-verifier", "false", embeddedKafkaBroker);

        KafkaAvroDeserializer valueDeserializer =
                new KafkaAvroDeserializer();
        consumerProperties.put("schema.registry.url", "http://localhost:8081");
        consumerProperties.put("specific.avro.reader", true);

        try (Consumer<String, Object> dltConsumer =
                     new DefaultKafkaConsumerFactory<String, Object>(
                             consumerProperties,
                             new StringDeserializer(),
                             valueDeserializer
                     ).createConsumer()) {

            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(dltConsumer, DLT_TOPIC);

            OrderCreatedEvent failedEvent = failedEvent();
            kafkaTemplate.send(SOURCE_TOPIC, String.valueOf(failedEvent.getOrderId()), failedEvent).join();

            ConsumerRecord<String, Object> dltRecord =
                    KafkaTestUtils.getSingleRecord(
                            dltConsumer,
                            DLT_TOPIC,
                            Duration.ofSeconds(15)
                    );

            OrderCreatedEvent dltEvent =
                    (OrderCreatedEvent) dltRecord.value();

            assertThat(dltRecord.key()).isEqualTo("9099");
            assertThat(dltEvent.getOrderId()).isEqualTo(9099L);
            assertThat(dltEvent.getProductId()).isEqualTo(999L);
            assertThat(headerText(dltRecord, "kafka_dlt-exception-cause-fqcn"))
                    .isEqualTo(IllegalArgumentException.class.getName());

            assertThat(headerText(dltRecord, "kafka_dlt-exception-message"))
                    .contains("Inventory not found for productId=999");
            assertThat(headerText(dltRecord, "kafka_dlt-original-topic"))
                    .isEqualTo(SOURCE_TOPIC);
        }
    }

    private OrderCreatedEvent failedEvent() {
        return OrderCreatedEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(9099L)
                .setProductId(999L)
                .setQuantity(1)
                .setAmount(new BigDecimal("500.00"))
                .setStatus("CREATED")
                .setOccurredAt(Instant.now())
                .build();
    }

    private String headerText(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        assertThat(header).as("Kafka header %s", name).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
