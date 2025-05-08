package com.microservices.poc.x.to.kafka.service;

import com.microservices.poc.config.XToKafkaServiceConfigData;
import com.microservices.poc.x.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.Arrays;

@SpringBootApplication
@ComponentScan(basePackages = {"com.microservices.poc"})
public class XToKafkaServiceApplication  implements CommandLineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(XToKafkaServiceApplication.class);
    private final XToKafkaServiceConfigData xToKafkaServiceConfigData;
    private final StreamRunner streamRunner;

    public XToKafkaServiceApplication(XToKafkaServiceConfigData xToKafkaServiceConfigData, StreamRunner streamRunner) {
        this.xToKafkaServiceConfigData = xToKafkaServiceConfigData;
        this.streamRunner = streamRunner;
    }

    public static void main(String[] args) {
        SpringApplication.run(XToKafkaServiceApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOGGER.info("Application starts...");
        LOGGER.info(Arrays.toString(xToKafkaServiceConfigData.getXKeywords().toArray(new String[] {})));
        LOGGER.info("Welcome message: {}", xToKafkaServiceConfigData.getWelcomeMessage());
        streamRunner.start();
    }
}
