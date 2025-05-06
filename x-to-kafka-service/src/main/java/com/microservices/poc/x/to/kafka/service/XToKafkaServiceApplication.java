package com.microservices.poc.x.to.kafka.service;

import com.microservices.poc.x.to.kafka.service.config.XToKafkaServiceConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@SpringBootApplication
public class XToKafkaServiceApplication  implements CommandLineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(XToKafkaServiceApplication.class);

    private final XToKafkaServiceConfigData xToKafkaServiceConfigData;

    public XToKafkaServiceApplication(XToKafkaServiceConfigData xToKafkaServiceConfigData) {
        this.xToKafkaServiceConfigData = xToKafkaServiceConfigData;
    }

    public static void main(String[] args) {
        SpringApplication.run(XToKafkaServiceApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOGGER.info("Application starts...");
        LOGGER.info(Arrays.toString(xToKafkaServiceConfigData.getXKeywords().toArray(new String[] {})));
        LOGGER.info("Welcome message: {}", xToKafkaServiceConfigData.getWelcomeMessage());
    }
}
