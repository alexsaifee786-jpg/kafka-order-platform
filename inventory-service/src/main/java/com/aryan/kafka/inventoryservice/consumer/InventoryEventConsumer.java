package com.aryan.kafka.inventoryservice.consumer;
import com.aryan.kafka.avro.OrderCreatedEvent;
import com.aryan.kafka.inventoryservice.service.InventoryProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.kafka.support.Acknowledgment;
@Component
public class InventoryEventConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(InventoryEventConsumer.class);
    private final InventoryProcessingService inventoryProcessingService;

    public InventoryEventConsumer(InventoryProcessingService inventoryProcessingService) {
        this.inventoryProcessingService = inventoryProcessingService;
    }

    @KafkaListener(topics = "${app.kafka.topics.order-created}")
    public void consume(ConsumerRecord<String, OrderCreatedEvent> record,Acknowledgment acknowledgment) {
        OrderCreatedEvent event = record.value();
        log.info(
                "Schema evolution check: orderId={}, source={}",
                event.getOrderId(),
                event.getSource()
        );
        log.info(
                "Order event received: key={}, event={}, partition={}, offset={}",
                record.key(),
                record.value(),
                record.partition(),
                record.offset()
        );
        boolean processed = inventoryProcessingService.process(event, record);

        if (processed) {
            log.info(
                    "Order event processed: orderId={}, productId={}, quantity={}",
                    event.getOrderId(),
                    event.getProductId(),
                    event.getQuantity()
            );
        } else {
            log.info(
                    "Duplicate event skipped: eventId={}",
                    event.getEventId()
            );
        }

        acknowledgment.acknowledge();
        log.info("Offset manually acknowledged: partition={},offset={}",record.partition(),record.offset());
    }
}