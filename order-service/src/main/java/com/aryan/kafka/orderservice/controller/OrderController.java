package com.aryan.kafka.orderservice.controller;

import com.aryan.kafka.orderservice.dto.CreateOrderRequest;
import com.aryan.kafka.orderservice.service.OrderApplicationService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderApplicationService orderApplicationService;

    public OrderController(OrderApplicationService orderApplicationService) {
        this.orderApplicationService = orderApplicationService;
    }

    @PostMapping
    public ResponseEntity<String> createOrder(
            @Valid @RequestBody CreateOrderRequest request) {

        Long orderId = orderApplicationService.createOrder(request);
return buildResponse(orderId);
    }

    private ResponseEntity<String> buildResponse(Long orderId) {

        String response = "Order accepted and queued for publishing"
                + ", orderId=" + orderId;

        return ResponseEntity.accepted().body(response);
    }
}