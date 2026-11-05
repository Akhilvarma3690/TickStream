package com.tickstream.simulator;

import com.tickstream.engine.IngestionEngine;
import com.tickstream.model.MarketTick;

import java.util.concurrent.ThreadLocalRandom;

public class MarketFeed implements Runnable{

    private final IngestionEngine engine;
    private volatile boolean running =true;
    private final String[] symbols={"APPL","MSFT","TSLA","GOOG","AMZN"};
    public MarketFeed(IngestionEngine engine){
        this.engine=engine;
    }
    @Override
    public void run(){
        ThreadLocalRandom random= ThreadLocalRandom.current();
        while(running){
            String symbol= symbols[random.nextInt(symbols.length)];
            double price=100 + (random.nextDouble()*50);
            long volume= random.nextLong(1,500);

            MarketTick tick= new MarketTick(symbol,price,volume,System.nanoTime());
            engine.ingest(tick);
        }
    }

    public void stop(){
        this.running=false;
    }
}
