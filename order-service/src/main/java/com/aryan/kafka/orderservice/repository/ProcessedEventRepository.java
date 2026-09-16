package com.aryan.kafka.orderservice.repository;

import com.aryan.kafka.orderservice.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent,Long> {
 boolean   existsByEventId(String eventId);
}
