package com.devs4j.kafka.transactional;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalProducer {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalProducer.class);

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("transactional.id", "devs4j-producer-id");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("linger.ms", "10");

    try (Producer<String, String> producer = new KafkaProducer<>(props)) { 

      try {
        producer.initTransactions();
        producer.beginTransaction();
        for (int i = 0; i < 100000; i++) {
          producer.send(new ProducerRecord<String,String>("devs4j-topic", String.valueOf(i), "devs4j-value"));

          if (i == 5000) throw new Exception("Unexpected exception");
        }
        producer.commitTransaction();
        producer.flush();
      } catch (Exception ex) {
        LOGGER.error("Error ", ex);
        producer.abortTransaction();
      }
    }
  }
}
