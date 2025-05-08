package com.microservices.poc.config;

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
    private Boolean enableMockTweets;
    private Long mockSleepMs;
    private Integer mockMinTweetLength;
    private Integer mockMaxTweetLength;
//    private String xV2BaseUrl;
//    private String xV2RulesBaseUrl;
//    private String xV2BearerToken;
}
