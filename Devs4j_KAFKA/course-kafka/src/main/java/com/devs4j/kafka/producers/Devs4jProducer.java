package com.devs4j.kafka.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Devs4jProducer {

  public static final Logger LOGGER = LoggerFactory.getLogger(Devs4jProducer.class);
  public static void main(String[] args) {
    Properties props = new Properties();
    /**
     * Brokers
    */
    props.put("bootstrap.servers", "localhost:9092");
    /*
     * Acknowledge
     * values = levels 0, 1, all
     * 0 = not care if is received or not
     * 1 = we need the acknowledge from one node, just to know if has been received
     * all = all nodes need to have the message and have to give a response
    */
    props.put("acks", "all");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    /*
     // Try-with-resources
     // he try-with-resources statement ensures 
     // that each resource is closed at the end of the statement. 
    try (Producer<String, String> producer = new KafkaProducer<>(props);) {
      producer.send(
        new ProducerRecord<>(
          "devs4j-topic",
          "devs4j-key",
          "devs4j-value"
        )
      );
    }

    // synchronous Messages - in order
    try (Producer<String, String> producer = new KafkaProducer<>(props);) {
      for(int i = 0; i < 10000; i++) {
        producer.send(
          new ProducerRecord<>(
            "devs4j-topic",
            String.valueOf(i),
            "devs4j-value"
          )
        ).get();
        producer.flush();
      } 
    } catch(InterruptedException | ExecutionException e) {
        LOGGER.error("Message producer interrupted ", e);
    }
    */

    
    // asynchronous Messages - order does not matter
    try (Producer<String, String> producer = new KafkaProducer<>(props);) {
      for(int i = 0; i < 100000; i++) {
        producer.send(
          new ProducerRecord<>(
            "devs4j-topic",
            String.valueOf(i),
            "devs4j-value"
          )
        );
        producer.flush();
      } 
    }
    
  }
}
