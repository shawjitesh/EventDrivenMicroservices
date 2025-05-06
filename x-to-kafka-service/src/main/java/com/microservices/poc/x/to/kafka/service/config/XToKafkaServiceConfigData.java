package com.microservices.poc.x.to.kafka.service.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@Configuration
@ConfigurationProperties(prefix = "x-to-kafka-service")
public class XToKafkaServiceConfigData {

    private List<String> xKeywords;
    private String welcomeMessage;
}
