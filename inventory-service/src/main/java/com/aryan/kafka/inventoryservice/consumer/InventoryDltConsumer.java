package com.aryan.kafka.inventoryservice.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class InventoryDltConsumer {
    private static final Logger log = LoggerFactory.getLogger(InventoryDltConsumer.class);
    private final KafkaAvroDeserializer avroDeserializer;

    public InventoryDltConsumer(@Value("${spring.kafka.consumer.properties.schema.registry.url}") String schemaRegistryUrl) {

        Map<String, Object> configs = new HashMap<>();
        configs.put("schema.registry.url", schemaRegistryUrl);
        configs.put("specific.avro.reader", true);

        this.avroDeserializer = new KafkaAvroDeserializer();
        this.avroDeserializer.configure(configs, false);
    }

    @KafkaListener(topics = "${app.kafka.topics.order-created}-dlt", groupId = "inventory-dlt-inspection-group", properties = {"value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer"})
    public void consume(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {

        byte[] payload = record.value();

        if (payload == null) {
            log.warn("DLT record has null value: key={}", record.key());
            acknowledgment.acknowledge();
            return;
        }

        try {
            Object decoded = avroDeserializer.deserialize(record.topic(), payload);

            if (decoded instanceof com.aryan.kafka.avro.OrderCreatedEvent) {

                com.aryan.kafka.avro.OrderCreatedEvent event = (com.aryan.kafka.avro.OrderCreatedEvent) decoded;

                log.info("DLT event: orderId={}, productId={}, quantity={}, amount={}, occurredAt={}", event.getOrderId(), event.getProductId(), event.getQuantity(), event.getAmount(), event.getOccurredAt());

            } else {
                log.warn("Unexpected DLT value type: key={}, type={}", record.key(), decoded == null ? "null" : decoded.getClass().getName());
            }

        } catch (Exception exception) {
            log.error("Could not decode DLT record: key={}, partition={}, offset={}, bytes={}", record.key(), record.partition(), record.offset(), payload.length, exception);
        }

        acknowledgment.acknowledge();
    }

    @jakarta.annotation.PreDestroy
    public void closeDeserializer() {
        avroDeserializer.close();
    }
}
