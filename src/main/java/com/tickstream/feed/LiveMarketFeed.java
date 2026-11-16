package com.tickstream.feed;

import com.tickstream.engine.IngestionEngine;
import com.tickstream.model.MarketTick;
import com.tickstream.producer.KafkaTickProducer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;

public class LiveMarketFeed implements WebSocket.Listener {
    //LIVE TRADE STREAM FOR BTC/USD
    private static final String COINBASE_WS_URL= "wss://ws-feed.exchange.coinbase.com";

    private static final String SUBSCRIBE_MSG = """
            {
                "type" : "subscribe",
                "product_ids" : ["BTC-USD"],
                "channels" : ["matches"]
            }
            """;
    private final CountDownLatch latch= new CountDownLatch(1);
    private final KafkaTickProducer kafkaProducer;
//    private final IngestionEngine engine;
    public LiveMarketFeed(KafkaTickProducer kafkaProducer){
        this.kafkaProducer=kafkaProducer;
    }

    public void  start() throws Exception{
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        System.out.println("Connecting to live exchange stream: "+ COINBASE_WS_URL);

        client.newWebSocketBuilder()
                .buildAsync(URI.create(COINBASE_WS_URL),this)
                .join();

        latch.await();
    }

    @Override
    public void onOpen(WebSocket webSocket){
        System.out.println("Connected to CoinBase! Subscribing to BTC-USD trade Channel ");
        //send subscription msg once socket is open
        webSocket.sendText(SUBSCRIBE_MSG,true);
        WebSocket.Listener.super.onOpen(webSocket);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket,CharSequence data,boolean last){
        String payload = data.toString();

        // Coinbase match event format: {"type":"match","price":"...","size":"...","product_id":"BTC-USD", ...}
        if(payload.contains("\"type\":\"match\"")) {
            String symbol = extractJsonField(payload, "\"product_id\":\"", "\"");
            String price = extractJsonField(payload, "\"price\":\"", "\"");
            String size = extractJsonField(payload, "\"size\":\"", "\"");
            if(!price.equals("UNKNOWN") && !size.equals("UNKNOWN")) {
                MarketTick tick = new MarketTick(
                        symbol,
                        Double.parseDouble(price),
                        (long) (Double.parseDouble(size)*100_000_000L),
                        System.nanoTime()
                );
                kafkaProducer.publish(tick);
//                System.out.printf("[PUBLISHED TO KAFKA] %s | $%s%n", tick.symbol(), tick.price());
            }


        }
        return  WebSocket.Listener.super.onText(webSocket,data,last);
    }

    @Override
    public void onError(WebSocket websocket,Throwable error){
        System.err.println("Websocket stream error: "+error.getMessage());
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket,int statusCode,String reason){
        System.out.println("WebSocket closed: " + reason);
        latch.countDown();
        return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
    }

    private String extractJsonField(String json,String startToken,String endToken){
        int start = json.indexOf(startToken);
        if(start == -1) return "UNKNOWN";
        start += startToken.length();
        int end=json.indexOf(endToken,start);
        return (end == -1) ? "UNKNOWN" : json.substring(start,end);
    }
}
