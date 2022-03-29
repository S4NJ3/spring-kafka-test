package com.devs4j.devs4jkafkasprint;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import io.micrometer.core.instrument.MeterRegistry;

@SpringBootApplication
//public class Devs4jKafkaSprintApplication implements CommandLineRunner {
public class Devs4jKafkaSprintApplication {

	private static final Logger LOGGER = LoggerFactory.getLogger(Devs4jKafkaSprintApplication.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private MeterRegistry meterRegistry;

	@KafkaListener(topics ="devs4j-topic",containerFactory ="listenerContainerFactory",groupId ="devs4j-group3", properties =	{"max.poll.interval.ms:60000","max.poll.records:10"})
	public void listen(List<ConsumerRecord<String, String>> messages) {

		for (ConsumerRecord<String, String> message : messages) {
			LOGGER.info("Partition = {}, Offset = {}, Key = {}, Value = {}", 
				message.partition(),
				message.offset(),
				message.key(),
				message.value()
			);
		}
		LOGGER.info("Batch completed");
	}

	public static void main(String[] args) {
		SpringApplication.run(Devs4jKafkaSprintApplication.class, args);
	}

	@Scheduled(fixedDelay = 2000, initialDelay = 100)
	public void sendKafkaMessages() {
		for (int i = 0; i < 100; i++) {
			kafkaTemplate.send("devs4j-topic", String.valueOf(i) ,String.format("SampleMessage %d", i));
		}
	}

	@Scheduled(fixedDelay = 2000, initialDelay = 500)
	public void printMetrics() {
		double count = meterRegistry.get("kafka.producer.record.send.total").functionCounter().count();
		LOGGER.info("Count {} ",count);
	}


/*
	/// RUN WITH CommandLineRunner
	@Override
	public void run(String... args) throws Exception {
		for (int i = 0; i < 100; i++) {
			kafkaTemplate.send("devs4j-topic", String.valueOf(i) ,String.format("SampleMessage %d", i));
		}

		//*************** */
		//SYNCHRONOUS MESSAGE
		// kafkaTemplate.send("devs4j-topic","Sample message").get(100, TimeUnit.MILLISECONDS); // If message is not sent by this time, it will generate TIMEOUT EXCEPTION
		/*
		 // ASYNCHRONOUS MESSAGE
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("devs4j-topic","Sample message");
		
		future.addCallback(new KafkaSendCallback<String,String> (){

			@Override
			public void onSuccess(SendResult<String, String> result) {
				LOGGER.info("Message sent ", result.getRecordMetadata().offset());
				
			}

			@Override
			public void onFailure(Throwable ex) {
				// TODO Auto-generated method stub
				LOGGER.error("Error sending message ", ex);
			}

			@Override
			public void onFailure(KafkaProducerException ex) {
				// TODO Auto-generated method stub
				LOGGER.error("Error sending message ", ex);
			}
			
		});
		*/
	//}



}
