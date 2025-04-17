package com.example.payment;

public record FraudEvent(
    String eventType,
    String userId,
    double totalAmount,
    long windowStartMillis
) {}
