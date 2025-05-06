package com.microservices.poc.x.to.kafka.service.runner.impl;

import com.microservices.poc.x.to.kafka.service.config.XToKafkaServiceConfigData;
import com.microservices.poc.x.to.kafka.service.listener.XKafkaStatusListener;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
@ConditionalOnExpression("${x-to-kafka-service.enable-v2-tweets} && not ${x-to-kafka-service.enable-mock-tweets}")
public class XV2StreamHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(XV2StreamHelper.class);
    private final XToKafkaServiceConfigData xToKafkaServiceConfigData;
    private final XKafkaStatusListener xKafkaStatusListener;
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final String TWEET_AS_RAW_JSON = "{" +
            "\"created_at\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}";
    private static final String X_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    public XV2StreamHelper(XToKafkaServiceConfigData xToKafkaServiceConfigData,
                           XKafkaStatusListener xKafkaStatusListener) {
        this.xToKafkaServiceConfigData = xToKafkaServiceConfigData;
        this.xKafkaStatusListener = xKafkaStatusListener;
    }

    /*
     * This method calls the filtered stream endpoint and streams Tweets from it
     * */
    void connectStream(String bearerToken) throws IOException, URISyntaxException, JSONException, InterruptedException {

        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(xToKafkaServiceConfigData.getXV2BaseUrl()))
                .header("Authorization", "Bearer " + bearerToken)
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        String[] lines = response.body().split("\n");

        for (String line : lines) {
            if (!line.isEmpty()) {
                String tweet = getFormattedTweet(line);
                Status status = null;
                try {
                    status = TwitterObjectFactory.createStatus(tweet);
                } catch (TwitterException e) {
                    LOGGER.error("Could not create status for text: {}", tweet, e);
                }
                if (status != null) {
                    xKafkaStatusListener.onStatus(status);
                }
            }
        }
    }

    /*
     * Helper method to setup rules before streaming data
     * */
    void setupRules(String bearerToken, Map<String, String> rules) throws IOException, URISyntaxException,
            InterruptedException {
        List<String> existingRules = getRules(bearerToken);
        if (!existingRules.isEmpty()) {
            deleteRules(bearerToken, existingRules);
        }
        createRules(bearerToken, rules);
        LOGGER.info("Created rules for twitter stream {}", rules.keySet());
    }


    /*
     * Helper method to create rules for filtering
     * */
    private void createRules(String bearerToken, Map<String, String> rules) throws URISyntaxException, IOException,
            InterruptedException {
        String requestBody = getFormattedString(rules);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(xToKafkaServiceConfigData.getXV2RulesBaseUrl()))
                .header("Authorization", "Bearer " + bearerToken)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /*
     * Helper method to get existing rules
     * */
    private List<String> getRules(String bearerToken) throws URISyntaxException, IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(xToKafkaServiceConfigData.getXV2RulesBaseUrl()))
                .header("Authorization", "Bearer " + bearerToken)
                .header("Content-Type", "application/json")
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        List<String> rules = new ArrayList<>();

        JSONObject json = new JSONObject(response.body());
        if (json.has("data")) {
            JSONArray array = json.getJSONArray("data");
            for (int i = 0; i < array.length(); i++) {
                rules.add(array.getJSONObject(i).getString("id"));
            }
        }
        return rules;
    }

    /*
     * Helper method to delete rules
     * */
    private void deleteRules(String bearerToken, List<String> existingRules) throws URISyntaxException, IOException,
            InterruptedException {
        String requestBody = getFormattedString(existingRules);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(xToKafkaServiceConfigData.getXV2RulesBaseUrl()))
                .header("Authorization", "Bearer " + bearerToken)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private String getFormattedString(List<String> ids) {
        return ids.size() == 1 ? String.format("{\"delete\": { \"ids\": [%s]}}", "\"" + ids.getFirst() + "\"") :
                String.format("{\"delete\": { \"ids\": [%s]}}", String.join(",",
                        ids.stream().map(id -> "\"" + id + "\"").toList()));
    }

    private String getFormattedString(Map<String, String> rules) {
        return rules.size() == 1
                ? String.format("{\"add\": [%s]}", "{\"value\": \"" + rules.keySet().iterator().next() +
                "\", \"tag\": \"" + rules.values().iterator().next() + "\"}")
                : String.format("{\"add\": [%s]}", rules.entrySet().stream()
                .map(e -> "{\"value\": \"" + e.getKey() + "\", \"tag\": \"" + e.getValue() + "\"}")
                .toList());
    }

    private String getFormattedTweet(String data) {
        JSONObject jsonData = new JSONObject(data).getJSONObject("data");

        String[] params = {
                ZonedDateTime.parse(jsonData.getString("created_at"))
                        .format(DateTimeFormatter.ofPattern(X_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                jsonData.getString("id"),
                jsonData.getString("text").replaceAll("\"", "\\\\\""),
                jsonData.getString("author_id")
        };
        return formatTweetAsJsonWithParams(params);
    }

    private String formatTweetAsJsonWithParams(String[] params) {
        String tweet = TWEET_AS_RAW_JSON;
        for (int i = 0; i < params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }
}
