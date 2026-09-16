package com.aryan.kafka.orderservice.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public class CreateOrderRequest {
    @NotNull
    @Positive
    private Long productId;

    @NotNull
    @Positive
    private Integer quantity;

    @NotNull
    @DecimalMin(value = "0.01")
    private BigDecimal amount;

    public CreateOrderRequest() {
    }

    public CreateOrderRequest(
            Long productId,
            Integer quantity,
            BigDecimal amount) {
        this.productId = productId;
        this.quantity = quantity;
        this.amount = amount;
    }
    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }
}