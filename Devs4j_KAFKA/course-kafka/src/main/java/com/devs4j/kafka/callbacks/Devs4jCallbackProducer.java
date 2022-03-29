package com.devs4j.kafka.callbacks;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Devs4jCallbackProducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(Devs4jCallbackProducer.class);
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    
    /*
    // Using callback
    try (Producer<String, String> producer = new KafkaProducer<>(props);) {
      for(int i = 0; i < 100; i++) {
        producer.send(
          new ProducerRecord<>(
            "devs4j-topic",
            String.valueOf(i),
            "devs4j-value"
          ), new Callback(){
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
              if(exception != null) {
                LOGGER.info("There was an error {}", exception.getMessage());
              }
              LOGGER.info("Offset = {}, Partition = {}, Topic = {}", metadata.offset(), metadata.partition(), metadata.topic());
            }
            
          }
        );
        producer.flush();
      } 
    }
    */

    // Using lambda
    try (Producer<String, String> producer = new KafkaProducer<>(props);) {
      for(int i = 0; i < 100; i++) {
        producer.send(
          new ProducerRecord<>(
            "devs4j-topic",
            String.valueOf(i),
            "devs4j-value"
          ), (metadata, exception) -> {
           
              if(exception != null) {
                LOGGER.info("There was an error {}", exception.getMessage());
              }
              LOGGER.info("Offset = {}, Partition = {}, Topic = {}", metadata.offset(), metadata.partition(), metadata.topic());
            
            
          }
        );
        producer.flush();
      } 
    }
  }
}
