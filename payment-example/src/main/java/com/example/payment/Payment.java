package com.example.payment;

/**
 * Immutable payment record. Java 17+ record generates
 * constructor, accessors, equals(), hashCode(), toString().
 */
public record Payment(
    String paymentId,
    String userId,
    long   timestamp,
    double amount,
    String currency,
    String merchant
) {}
