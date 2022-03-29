package com.devs4j.kafka.consumers;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dev4jConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(Dev4jConsumer.class);
  public static void main(String[] args) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("group.id", "devs4j-group"); // id for consumer's group
    props.setProperty("enable.auto.commit", "true");
    props.setProperty("auto.commit.intervals.ms", "1000");
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    try(KafkaConsumer<String, String> kConsumer = new KafkaConsumer<>(props)) {
      kConsumer.subscribe(Arrays.asList("devs4j-topic"));

      while (true) {
        ConsumerRecords<String, String> consumerRecords = kConsumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
          LOGGER.info(
            "Offset = {} ; Partition = {} ; Key = {} ; Value = {}",
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
