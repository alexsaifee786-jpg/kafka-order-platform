package com.aryan.kafka.orderservice.repository;

import com.aryan.kafka.orderservice.entity.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {
}