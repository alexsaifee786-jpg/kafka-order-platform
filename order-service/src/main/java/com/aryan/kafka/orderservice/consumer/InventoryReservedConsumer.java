package com.aryan.kafka.orderservice.consumer;

import com.aryan.kafka.orderservice.event.InventoryReservedEvent;
import com.aryan.kafka.orderservice.service.OrderApplicationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class InventoryReservedConsumer {
    private static final Logger log= LoggerFactory.getLogger(InventoryReservedConsumer.class);
    private OrderApplicationService orderApplicationService;

    public InventoryReservedConsumer(OrderApplicationService orderApplicationService) {
        this.orderApplicationService = orderApplicationService;
    }
    @KafkaListener(topics = "${app.kafka.topics.inventory-reserved}")
    public void consume(ConsumerRecord<String, InventoryReservedEvent> record, Acknowledgment acknowledgment)
    {
        InventoryReservedEvent event = record.value();
        log.info("Inventory reservation received: orderId={} status={},partition={},offset={}",
                event.getOrderId()
                ,event.getStatus(),
                record.partition(),
                record.offset());
        boolean processed = orderApplicationService.markInventoryReserved(event, record);
        if (processed)
        {
            log.info("Order status updated: orderId={}, status={}",
                    event.getOrderId(),
                    "INVENTORY_RESERVED");
        }
        else {
            log.info(
                    "Duplicate inventory reservation skipped: eventId={}",
                    event.getEventId()
            );
        }
        acknowledgment.acknowledge();
        log.info(
                "Inventory reservation offset acknowledged: orderId={}, partition={}, offset={}",
                event.getOrderId(),
                record.partition(),
                record.offset()
        );
    }
}
