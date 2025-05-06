package com.microservices.poc.x.to.kafka.service.runner.impl;

import com.microservices.poc.x.to.kafka.service.config.XToKafkaServiceConfigData;
import com.microservices.poc.x.to.kafka.service.listener.XKafkaStatusListener;
import com.microservices.poc.x.to.kafka.service.runner.StreamRunner;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import java.util.Arrays;

@Component
@ConditionalOnExpression("${twitter-to-kafka-service.enable-mock-tweets} && " +
        "not ${twitter-to-kafka-service.enable-v2-tweets}")
public class XKafkaStreamRunner implements StreamRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(XKafkaStreamRunner.class);
    private final XToKafkaServiceConfigData xToKafkaServiceConfigData;
    private final XKafkaStatusListener xKafkaStatusListener;
    private TwitterStream twitterStream;

    public XKafkaStreamRunner(XToKafkaServiceConfigData xToKafkaServiceConfigData,
                              XKafkaStatusListener xKafkaStatusListener) {
        this.xToKafkaServiceConfigData = xToKafkaServiceConfigData;
        this.xKafkaStatusListener = xKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(xKafkaStatusListener);
        addFilter();
    }

    private void addFilter() {
        String[] keywords = xToKafkaServiceConfigData.getXKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        LOGGER.info("Started filtering twitter stream for keywords {}", Arrays.toString(keywords));
    }

    @PreDestroy
    public void shutdown() {
        if (twitterStream != null) {
            LOGGER.info("Closing twitter stream");
            twitterStream.shutdown();
        }
    }
}
