package com.aryan.kafka.inventoryservice;

import com.aryan.kafka.inventoryservice.entity.Inventory;
import com.aryan.kafka.avro.OrderCreatedEvent;
import com.aryan.kafka.inventoryservice.repository.InventoryRepository;
import com.aryan.kafka.inventoryservice.repository.ProcessedEventRepository;
import com.aryan.kafka.inventoryservice.service.InventoryProcessingService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import javax.lang.model.element.ElementVisitor;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:mysql://localhost:3306/inventory_test_db",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.kafka.listener.auto-startup=false"
})
@ActiveProfiles("test")
public class InventoryDatabaseIntegrationTest {
    @Autowired
    private InventoryProcessingService  inventoryProcessingService;
    @Autowired
    private InventoryRepository inventoryRepository;
    @Autowired
  private   ProcessedEventRepository processedEventRepository;
    @BeforeEach
    void cleanDatabase()
    {
        processedEventRepository.deleteAll();
        inventoryRepository.deleteAll();
    }
    @Test
    void shouldUpdateInventoryAndSaveProcessedEvent() {
        inventoryRepository.save(new Inventory(501L,10, LocalDateTime.now()));
        OrderCreatedEvent event=new OrderCreatedEvent();
        event.setEventId("evt-integration-5001");
        event.setOrderId(5001L);
        event.setProductId(501L);
        event.setQuantity(2);
        ConsumerRecord<String, OrderCreatedEvent> record =
                new ConsumerRecord<>(
                        "orders.created",
                        0,
                        20L,
                        "5001",
                        event
                );
        boolean processed = inventoryProcessingService.process(event, record);
        Inventory savedInventory = inventoryRepository.findByProductId(501L).orElseThrow();
assertTrue(processed);
assertEquals(8,savedInventory.getAvailableStock());
assertTrue(processedEventRepository.existsByEventId("evt-integration-5001"));
    }
}
