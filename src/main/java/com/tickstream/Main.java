package com.tickstream;

import com.tickstream.engine.IngestionEngine;
import com.tickstream.feed.LiveMarketFeed;
import com.tickstream.metrics.TickMetrics;
import com.tickstream.producer.KafkaTickProducer;
import com.tickstream.simulator.MarketFeed;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception{
//        System.out.println("Starting TickStream Distributed Gateway ...");
//        // Dependency Injection
//        TickMetrics metrics = new TickMetrics();
//        //QueueSize : 10000 , Worker threads : 4
//        IngestionEngine engine = new IngestionEngine(10000,4,metrics);
//        MarketFeed feed=new MarketFeed(engine);
//
//        //start telemetry reporter thread
//        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
//        reporter.scheduleAtFixedRate(() ->{
//           long processed = metrics.getProcessedAndReset();
//           long dropped = metrics.getDroppedAndReset();
//           System.out.printf("[Metrics] Processed/sec: %d | Dropped/sec: %d%n",processed,dropped);
//        },1,1, TimeUnit.SECONDS);
//
//
//        //graceful shutdown for ctrl-c
//        Runtime.getRuntime().addShutdownHook(new Thread(()->{
//            System.out.println("Initiating Graceful ShutDown...");
//            reporter.shutdown();
//            engine.shutdown();
//        }));

        System.out.println("Starting TickStream Distributed Gateway...");

        // 1. Initialize Kafka Producer targeting the "market-ticks" topic
        KafkaTickProducer producer = new KafkaTickProducer("market-ticks");

        // 2. Register graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nInitiating Graceful ShutDown...");
            producer.shutdown();
        }));
        //start market feed on its own thread
        LiveMarketFeed feedThread =  new LiveMarketFeed(producer);
        feedThread.start();
    }
}
