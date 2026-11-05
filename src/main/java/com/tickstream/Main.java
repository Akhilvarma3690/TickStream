package com.tickstream;

import com.tickstream.engine.IngestionEngine;
import com.tickstream.metrics.TickMetrics;
import com.tickstream.simulator.MarketFeed;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args){
        System.out.println("Starting TickStream Engine ...");
        // Dependency Injection
        TickMetrics metrics = new TickMetrics();
        //QueueSize : 10000 , Worker threads : 4
        IngestionEngine engine = new IngestionEngine(10000,4,metrics);
        MarketFeed feed=new MarketFeed(engine);

        //start telemetry reporter thread
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
        reporter.scheduleAtFixedRate(() ->{
           long processed = metrics.getProcessedAndReset();
           long dropped = metrics.getDroppedAndReset();
           System.out.printf("[Metrics] Processed/sec: %d | Dropped/sec: %d%n",processed,dropped);
        },1,1, TimeUnit.SECONDS);

        //start market feed on its own thread
        Thread feedThread =  new Thread(feed,"market-feed-thread");
        feedThread.start();

        //graceful shutdown for ctrl-c
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            System.out.println("Initiating Graceful ShutDown...");
            feed.stop();
            reporter.shutdown();
            engine.shutdown();
        }));
    }
}
