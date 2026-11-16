package com.tickstream.producer;

import com.tickstream.model.MarketTick;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class KafkaTickProducer {
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaTickProducer(String topic){
        this.topic= topic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //wait upto 5ms to group ticks into single tcp packet
        props.put(ProducerConfig.LINGER_MS_CONFIG,5);
        //max batch size per partition (64Kb)
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,65536);
        //compress batches using snappy (optimized for cpu cache)
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        props.put(ProducerConfig.ACKS_CONFIG,"1");
        this.producer=new KafkaProducer<>(props);


    }
    public void publish(MarketTick tick){
        String  payload = tick.price()+","+ tick.volume()+","+ tick.timestampNs();
        ProducerRecord<String,String> record=new ProducerRecord<>(topic,tick.symbol(),payload);
        //Asynchronus send
        producer.send(record,(metadata,exception)->{
            if(exception !=null){
                System.err.println("Failed to send tick to kafka: "+exception.getMessage());
            }else {
                // Only prints when Kafka physically acknowledges the write to disk!
                System.out.printf("[KAFKA ACK] %s | Partition: %d | Offset: %d%n",
                        tick.symbol(), metadata.partition(), metadata.offset());
            }
        });
    }
    public  void shutdown(){
        System.out.println("Flush remaining kafka buffers ");
        producer.flush();
        producer.close();
    }

}
