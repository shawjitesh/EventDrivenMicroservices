package com.microservices.poc.x.to.kafka.service.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class XKafkaStatusListener extends StatusAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(XKafkaStatusListener.class);

    @Override
    public void onStatus(Status status) {
        LOGGER.info("X status with text {}", status.getText());
    }
}
