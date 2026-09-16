package com.aryan.kafka.inventoryservice;

import com.aryan.kafka.inventoryservice.event.OrderCreatedEvent;
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
@EmbeddedKafka(partitions = 3, topics = {"orders.created", "orders.created-dlt"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.group-id=inventory-dlt-integration-test"
})
class InventoryRetryToDltIntegrationTests {

    private static final String SOURCE_TOPIC = "orders.created";
    private static final String DLT_TOPIC = "orders.created-dlt";

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void failedInventoryEventIsPublishedToDltWithFailureMetadata() {
        Map<String, Object> consumerProperties =
                KafkaTestUtils.consumerProps("dlt-verifier", "false", embeddedKafkaBroker);

        JsonDeserializer<OrderCreatedEvent> valueDeserializer =
                new JsonDeserializer<>(OrderCreatedEvent.class, false);

        try (Consumer<String, OrderCreatedEvent> dltConsumer =
                     new DefaultKafkaConsumerFactory<>(
                             consumerProperties,
                             new StringDeserializer(),
                             valueDeserializer
                     ).createConsumer()) {

            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(dltConsumer, DLT_TOPIC);

            OrderCreatedEvent failedEvent = failedEvent();
            kafkaTemplate.send(SOURCE_TOPIC, String.valueOf(failedEvent.getOrderId()), failedEvent).join();

            ConsumerRecord<String, OrderCreatedEvent> dltRecord =
                    KafkaTestUtils.getSingleRecord(dltConsumer, DLT_TOPIC, Duration.ofSeconds(15));

            assertThat(dltRecord.key()).isEqualTo("9099");
            assertThat(dltRecord.value().getOrderId()).isEqualTo(9099L);
            assertThat(dltRecord.value().getProductId()).isEqualTo(999L);
            assertThat(headerText(dltRecord, "kafka_dlt-exception-cause-fqcn"))
                    .isEqualTo(RuntimeException.class.getName());
            assertThat(headerText(dltRecord, "kafka_dlt-exception-message"))
                    .contains("Inventory processing failed for productId=999");
            assertThat(headerText(dltRecord, "kafka_dlt-original-topic"))
                    .isEqualTo(SOURCE_TOPIC);
        }
    }

    private OrderCreatedEvent failedEvent() {
        OrderCreatedEvent event = new OrderCreatedEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setOrderId(9099L);
        event.setProductId(999L);
        event.setQuantity(1);
        event.setAmount(new BigDecimal("500.00"));
        event.setStatus("CREATED");
        event.setOccurredAt(Instant.now());
        return event;
    }

    private String headerText(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        assertThat(header).as("Kafka header %s", name).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
