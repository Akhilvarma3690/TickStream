package com.tickstream.model;

public record MarketTick(
        String symbol,
        double price,
        long volume,
        long timestampNs
) {
}
