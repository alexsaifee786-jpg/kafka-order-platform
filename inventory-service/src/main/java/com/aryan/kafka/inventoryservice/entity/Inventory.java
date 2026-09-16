package com.aryan.kafka.inventoryservice.entity;

import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "inventory")
public class Inventory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Version
private Long version;
    @Column(name = "product_id", nullable = false, unique = true)
    private Long productId;

    @Column(name = "available_stock", nullable = false)
    private Integer availableStock;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    public Inventory() {
    }

    public Inventory(
            Long productId,
            Integer availableStock,
            LocalDateTime updatedAt) {

        this.productId = productId;
        this.availableStock = availableStock;
        this.updatedAt = updatedAt;
    }

    public Long getId() {
        return id;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Integer getAvailableStock() {
        return availableStock;
    }

    public void setAvailableStock(Integer availableStock) {
        this.availableStock = availableStock;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}