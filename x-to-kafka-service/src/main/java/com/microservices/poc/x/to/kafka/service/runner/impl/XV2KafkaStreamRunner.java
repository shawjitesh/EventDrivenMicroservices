//package com.microservices.poc.x.to.kafka.service.runner.impl;
//
//import com.microservices.poc.x.to.kafka.service.config.XToKafkaServiceConfigData;
//import com.microservices.poc.x.to.kafka.service.runner.StreamRunner;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
//import org.springframework.stereotype.Component;
//
//import java.io.IOException;
//import java.net.URISyntaxException;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//@Component
//@ConditionalOnExpression("${x-to-kafka-service.enable-v2-tweets} && " +
//        "not ${x-to-kafka-service.enable-mock-tweets}")
//public class XV2KafkaStreamRunner implements StreamRunner {
//
//    private static final Logger LOG = LoggerFactory.getLogger(XV2KafkaStreamRunner.class);
//    private final XToKafkaServiceConfigData xToKafkaServiceConfigData;
//    private final XV2StreamHelper xV2StreamHelper;
//
//    public XV2KafkaStreamRunner(XToKafkaServiceConfigData xToKafkaServiceConfigData,
//                                XV2StreamHelper xV2StreamHelper) {
//        this.xToKafkaServiceConfigData = xToKafkaServiceConfigData;
//        this.xV2StreamHelper = xV2StreamHelper;
//    }
//
//    @Override
//    public void start() {
//        String bearerToken = xToKafkaServiceConfigData.getXV2BearerToken();
//        if (null != bearerToken) {
//            try {
//                xV2StreamHelper.setupRules(bearerToken, getRules());
//                xV2StreamHelper.connectStream(bearerToken);
//            } catch (IOException | URISyntaxException | InterruptedException e) {
//                LOG.error("Error streaming tweets!", e);
//                throw new RuntimeException("Error streaming tweets!", e);
//            }
//        } else {
//            LOG.error("There was a problem getting your bearer token. " +
//                    "Please make sure you set the TWITTER_BEARER_TOKEN environment variable");
//            throw new RuntimeException("There was a problem getting your bearer token. +" +
//                    "Please make sure you set the TWITTER_BEARER_TOKEN environment variable");
//        }
//
//    }
//
//    private Map<String, String> getRules() {
//        List<String> keywords = xToKafkaServiceConfigData.getXKeywords();
//        Map<String, String> rules = new HashMap<>();
//        for (String keyword: keywords) {
//            rules.put(keyword, "Keyword: " + keyword);
//        }
//        LOG.info("Created filter for twitter stream for keywords: {}", keywords);
//        return rules;
//    }
//}
