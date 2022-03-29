package com.devs4j.kafka.multithread;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Devs4jThreadConsumer extends Thread {

  private final KafkaConsumer<String, String> consumer;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private static final Logger LOGGER = LoggerFactory.getLogger(Devs4jThreadConsumer.class);

  public Devs4jThreadConsumer(KafkaConsumer<String, String> consumer) {
    this.consumer = consumer;
  }
  
  @Override
  public void run() {
   this.consumer.subscribe(Arrays.asList("devs4j-topic"));
    
      try {
        while(!this.closed.get()) {
          ConsumerRecords<String, String> consumerRecords = this.consumer.poll(Duration.ofMillis(100));
          for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            LOGGER.debug(
              "Offset = {} ; Partition = {} ; Key = {} ; Value = {}",
              consumerRecord.offset(),
              consumerRecord.partition(),
              consumerRecord.key(),
              consumerRecord.value()
            );

            if((Integer.parseInt(consumerRecord.key()) % 100000) == 0) {
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

      } catch(WakeupException e) {
        if(this.closed.get()) {
          throw e;
        }
      } finally {
        this.consumer.close();
      }
  }

  public void shutdown() {
    this.closed.set(true);
    this.consumer.wakeup();
  }

}
