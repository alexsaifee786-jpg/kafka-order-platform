package com.aryan.kafka.orderservice.consumer;

import com.aryan.kafka.orderservice.event.InventoryReservationFailedEvent;
import com.aryan.kafka.orderservice.service.OrderApplicationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class InventoryReservationFailedConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(InventoryReservationFailedConsumer.class);

    private final OrderApplicationService orderApplicationService;

    public InventoryReservationFailedConsumer(
            OrderApplicationService orderApplicationService) {
        this.orderApplicationService = orderApplicationService;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-reservation-failed}",
            properties = {
                    "spring.json.use.type.headers=false",
                    "spring.json.value.default.type=com.aryan.kafka.orderservice.event.InventoryReservationFailedEvent"
            }
    )
    public void consume(
            ConsumerRecord<String, InventoryReservationFailedEvent> record,
            Acknowledgment acknowledgment) {

        InventoryReservationFailedEvent event = record.value();

        boolean processed =
                orderApplicationService.markInventoryRejected(event, record);

        if (processed) {
            log.info(
                    "Order rejected: orderId={}, reason={}",
                    event.getOrderId(),
                    event.getReason()
            );
        } else {
            log.info(
                    "Duplicate inventory rejection skipped: eventId={}",
                    event.getEventId()
            );
        }

        acknowledgment.acknowledge();

        log.info(
                "Inventory rejection offset acknowledged: partition={}, offset={}",
                record.partition(),
                record.offset()
        );
    }
}