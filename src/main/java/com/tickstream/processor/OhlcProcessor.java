package com.tickstream.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class OhlcProcessor {
    public static void main(String[] args){
        System.out.println("Starting TickStream OHLC Processor...");
        Properties props=new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tickstream-ohlc-engine-v4");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        //Disable Cache: Force Kafka Streams to output the aggregation on every single tick
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_DOC,0);
        //2. READ HISTORY: Force the consumer to read the topic from the beginning to instantly catch up
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        StreamsBuilder builder = new StreamsBuilder();
        // Ingest raw tick stream
        KStream<String,String> ticks=builder.stream("market-ticks");
        // Aggregate into 1-minute Tumbling Windows
        ticks.peek((key, value) -> System.out.println("[RAW DEBUG] " + key + " -> " + value))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)));

        ticks.groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .aggregate(
                        () -> "",
                        (symbol, tickValue, aggValue) -> {
                            try {
                                String[] tick = tickValue.split(",");
                                double price = Double.parseDouble(tick[0]);
                                long volume = Long.parseLong(tick[1]);

                                if (aggValue.isEmpty()) {
                                    return price + "," + price + "," + price + "," + price + "," + volume;
                                } else {
                                    String[] agg = aggValue.split(",");
                                    double open = Double.parseDouble(agg[0]);
                                    double high = Math.max(Double.parseDouble(agg[1]), price);
                                    double low = Math.min(Double.parseDouble(agg[2]), price);
                                    double close = price;
                                    long totalVol = Long.parseLong(agg[4]) + volume;

                                    return open + "," + high + "," + low + "," + close + "," + totalVol;
                                }
                            } catch (Exception e) {
                                System.err.println("[POISON PILL] Skipping bad data: " + tickValue);
                                return aggValue; // Return previous state safely
                            }
                        },
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                .toStream()
                //print live rolling candlestick to terminal
                .foreach((windowedKey,ohlc)->{
                    String[] agg=ohlc.split(",");
                    System.out.printf("[%s] OHLC-1m | O: %s | H: %s | L: %s | C: %s | Vol: %s%n",
                            windowedKey.key(), agg[0], agg[1], agg[2], agg[3], agg[4]);
                });
        // --- 5-MINUTE HOPPING VWAP AGGREGATION ---
        // Reads from the exact same 'ticks' source variable you already defined
        ticks.groupByKey()
                // 5-minute window size, moving forward by 1 minute at a time
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1)))
                .aggregate(
                        () -> "0.0,0", // Initial state: Cumulative (Price * Volume), Cumulative Volume
                        (symbol, tickValue, aggValue) -> {
                            try {
                                String[] tick = tickValue.split(",");
                                double price = Double.parseDouble(tick[0]);
                                long volume = Long.parseLong(tick[1]);

                                String[] agg = aggValue.split(",");
                                double cumulativeDollar = Double.parseDouble(agg[0]);
                                long cumulativeVol = Long.parseLong(agg[1]);

                                // Math: Add current tick's (price * volume) to the total pool
                                cumulativeDollar += (price * volume);
                                // Math: Add current tick's volume to the total pool
                                cumulativeVol += volume;

                                return cumulativeDollar + "," + cumulativeVol;
                            } catch (Exception e) {
                                return aggValue; // Ignore poison pills
                            }
                        },
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                .toStream()
                .foreach((windowedKey, vwapData) -> {
                    String[] agg = vwapData.split(",");
                    double cumulativeDollar = Double.parseDouble(agg[0]);
                    long cumulativeVol = Long.parseLong(agg[1]);

                    if (cumulativeVol > 0) {
                        double vwap = cumulativeDollar / cumulativeVol;
                        System.out.printf("[%s] VWAP-5m | True Avg Price: $%.2f | Total Vol: %d%n",
                                windowedKey.key(), vwap, cumulativeVol);
                    }
                });

        //start the engine
        KafkaStreams streams= new KafkaStreams(builder.build(),props);
        streams.cleanUp();

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            streams.close();
            latch.countDown();
        }));

        try{
            streams.start();
            //block main thread inidefinitely
            latch.await();
        } catch (InterruptedException e) {
            System.exit(1);
        }

    }
}
