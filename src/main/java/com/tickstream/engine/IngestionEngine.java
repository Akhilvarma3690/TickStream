package com.tickstream.engine;

import com.tickstream.metrics.TickMetrics;
import com.tickstream.model.MarketTick;

import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

public class IngestionEngine {
    private final BlockingQueue<Runnable> ringBuffer;
    private final ThreadPoolExecutor workerPool;
    private final TickMetrics metrics;
    public IngestionEngine(int queueCapacity,int workerThreads,TickMetrics metrics){
        this.metrics=metrics;
        //strictly sized buffer to prevent outOfMemoryError
        this.ringBuffer = new ArrayBlockingQueue<>(queueCapacity);
        //fixed worker pool to prevent context switching overload
        this.workerPool = new ThreadPoolExecutor(workerThreads,
                workerThreads,
                0L,
                TimeUnit.MILLISECONDS,
                this.ringBuffer,
                new ThreadFactory() {
                    private int counter = 0;
                    @Override
                    public Thread newThread(Runnable runnable) {
                        return new Thread(runnable, "tick-worker-"+ counter++);
            }
        }, //custom backpressure to gracefully drop excess data
                new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable runnable, ThreadPoolExecutor threadPoolExecutor) {
                metrics.recordDropped();
            }
        }
        );
    }
    //hand off payload to worker pool asynchronusly
    public void ingest(MarketTick tick){
        workerPool.execute(()-> processTick(tick));
    }

    public void processTick(MarketTick tick){
        //simulate lightweight processing
        if(tick.price()>0){
            metrics.recordProcessed();
        }
    }
    public void shutdown(){
        workerPool.shutdown();
    }


}

