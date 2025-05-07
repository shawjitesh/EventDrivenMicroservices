package com.microservices.poc.x.to.kafka.service.exception;

public class XToKafkaServiceException extends RuntimeException {

    public XToKafkaServiceException() {
    }

    public XToKafkaServiceException(String message) {
        super(message);
    }

    public XToKafkaServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}
