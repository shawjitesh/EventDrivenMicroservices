package com.microservices.poc.x.to.kafka.service.runner.impl;

import com.microservices.poc.x.to.kafka.service.config.XToKafkaServiceConfigData;
import com.microservices.poc.x.to.kafka.service.exception.XToKafkaServiceException;
import com.microservices.poc.x.to.kafka.service.listener.XKafkaStatusListener;
import com.microservices.poc.x.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "x-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockKafkaStreamRunner.class);
    private final XToKafkaServiceConfigData xToKafkaServiceConfigData;
    private final XKafkaStatusListener xKafkaStatusListener;
    private static final Random RANDOM = new Random();
    private static final String[] WORDS = new String[] {
            "LoremIpsum","is","simply","dummy","text","of","the","printing","and","typesetting","industry","Lorem",
            "Ipsum","has","been","the","industry's","standard","dummy","text","ever","since","the","1500s","when","an",
            "unknown","printer","took","a","galley","of","type","and","scrambled","it","to","make","a","type",
            "specimen","book","It","has","survived","not","only","five","centuries","but","also","the","leap","into",
            "electronic","typesetting","remaining","essentially","unchanged","It","was","popularized","in","the",
            "1960s","with","the","release","of","Letraset","sheets","containing","Lorem","Ipsum","passages","and",
            "more","recently","with","desktop","publishing","software","like","Aldus","PageMaker","including",
            "versions","of","Lorem","Ipsum"
    };
    private static final String TWEET_AS_RAW_JSON = "{" +
            "\"created_at\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}";
    private static final String X_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    public MockKafkaStreamRunner(XToKafkaServiceConfigData xToKafkaServiceConfigData,
                                 XKafkaStatusListener xKafkaStatusListener) {
        this.xToKafkaServiceConfigData = xToKafkaServiceConfigData;
        this.xKafkaStatusListener = xKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException {
        String[] keywords = xToKafkaServiceConfigData.getXKeywords().toArray(new String[0]);
        int minTweetLength = xToKafkaServiceConfigData.getMockMinTweetLength();
        int maxTweetLength = xToKafkaServiceConfigData.getMockMaxTweetLength();
        long mockSleepMs = xToKafkaServiceConfigData.getMockSleepMs();
        LOGGER.info("Starting mock filtering twitter streams with keywords: {}", Arrays.toString(keywords));
        simulateTwitterStream(keywords, minTweetLength, maxTweetLength, mockSleepMs);
    }

    private void simulateTwitterStream(String[] keywords, int minTweetLength, int maxTweetLength, long mockSleepMs) {
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while (true) {
                    String formattedTweetAsRawJson = getFormattedTweet(keywords, minTweetLength, maxTweetLength);
                    Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
                    xKafkaStatusListener.onStatus(status);
                    sleep(mockSleepMs);
                }
            } catch (TwitterException e) {
                LOGGER.error("Error creating twitter status!", e);
            }
        });
    }

    private void sleep(long mockSleepMs) {
        try {
            Thread.sleep(mockSleepMs);
        } catch (InterruptedException e) {
            throw new XToKafkaServiceException("Error while sleeping to wait for new status to create!", e);
        }
    }

    private String getFormattedTweet(String[] keywords, int minTweetLength, int maxTweetLength) {
        String[] params = new String[] {
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(X_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keywords, minTweetLength, maxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
        };

        return formatTweetAsJsonWithParams(params);
    }

    private static String formatTweetAsJsonWithParams(String[] params) {
        String tweet = TWEET_AS_RAW_JSON;
        for (int i = 0; i < params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minTweetLength, int maxTweetLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetLength - minTweetLength + 1) + minTweetLength;
        return constructRandomTweet(keywords, tweetLength, tweet);
    }

    private static String constructRandomTweet(String[] keywords, int tweetLength, StringBuilder tweet) {
        for (int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if (i == tweetLength / 2) {
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }
}
