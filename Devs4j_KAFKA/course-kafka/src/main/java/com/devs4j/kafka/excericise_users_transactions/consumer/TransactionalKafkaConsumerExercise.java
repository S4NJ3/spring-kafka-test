package com.devs4j.kafka.excericise_users_transactions.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalKafkaConsumerExercise {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalKafkaConsumerExercise.class);

  public static void main(String[] args) {
    Properties config = new Properties();
    config.setProperty("bootstrap.servers", "localhost:9092");
    config.setProperty("group.id", "devs4j-group");
    config.setProperty("enable.auto.commit", "true");
    config.setProperty("isolation.level", "read_committed");
    config.setProperty("auto.commit.interval.ms", "1000");
    config.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
      consumer.subscribe(Arrays.asList("exercise-transactions"));
      while (true) {
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
          LOGGER.info(
            "Offset = {}, Partition = {}, Key = {}, Value= {}",
            consumerRecord.offset(),
            consumerRecord.partition(),
            consumerRecord.key(),
            consumerRecord.value()
          );
        }
      }
    }
  }
}
