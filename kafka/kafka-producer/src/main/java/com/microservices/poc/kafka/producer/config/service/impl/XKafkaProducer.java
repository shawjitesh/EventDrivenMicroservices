package com.microservices.poc.kafka.producer.config.service.impl;

import com.microservices.poc.kafka.avro.model.XAvroModel;
import com.microservices.poc.kafka.producer.config.service.KafkaProducer;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class XKafkaProducer implements KafkaProducer<Long, XAvroModel> {

    private static final Logger LOGGER = LoggerFactory.getLogger(XKafkaProducer.class);

    private KafkaTemplate<Long, XAvroModel> kafkaTemplate;

    public XKafkaProducer(KafkaTemplate<Long, XAvroModel> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(String topicName, Long key, XAvroModel message) {
        LOGGER.info("Sending message={} to topic={}", message, topicName);
        CompletableFuture<SendResult<Long, XAvroModel>> kafkaResultFuture = kafkaTemplate.send(topicName, key, message);
        addCallback(topicName, message, kafkaResultFuture);
    }

    private static void addCallback(String topicName, XAvroModel message,
                                    CompletableFuture<SendResult<Long, XAvroModel>> kafkaResultFuture) {
        kafkaResultFuture.whenComplete((result, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error while sending message {} to topic {}", message, topicName, throwable);
            } else {
                RecordMetadata recordMetadata = result.getRecordMetadata();
                LOGGER.debug("Received new metadata. Topic: {}, Partition: {}, Offset: {}, Timestamp: {}, at time {}",
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset(),
                        recordMetadata.timestamp(),
                        System.nanoTime());
            }
        });
    }

    @PreDestroy
    public void close() {
        if (kafkaTemplate != null) {
            LOGGER.info("Closing Kafka producer!");
            kafkaTemplate.destroy();
        }
    }
}
