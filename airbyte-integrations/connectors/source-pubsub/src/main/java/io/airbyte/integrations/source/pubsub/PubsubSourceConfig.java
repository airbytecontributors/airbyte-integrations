package io.airbyte.integrations.source.pubsub;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.common.base.Charsets;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;

public class PubsubSourceConfig {
    private static final Logger logger = LoggerFactory.getLogger(PubsubSourceConfig.class.getName());
    private final ProjectSubscriptionName subscription;
    private String consumerThreadName;
    private final JsonNode config;
    private String connectorId;
    private SubscriptionAdminClient subscriptionAdminClient = null;
    private PullRequest checkPullrequest;

    public PubsubSourceConfig(String consumerThreadName, final JsonNode config, String connectorId) {
        this.consumerThreadName = consumerThreadName;
        this.config = config;
        this.connectorId = connectorId;
        final String subscriptionId = config.has("subscription_id") ? config.get("subscription_id").asText() : "";
        final String projectId = config.has("project_id") ? config.get("project_id").asText() : "";
        subscription = ProjectSubscriptionName.of(projectId, subscriptionId);
        subscriptionAdminClient = getConsumer();
        int maxNumberOfMessages = 10;
        checkPullrequest = PullRequest.newBuilder()
                .setMaxMessages(maxNumberOfMessages)
                .setSubscription(subscription.toString())
                .build();
    }

    public ProjectName getProjectName() {
        final String projectId = config.has("project_id") ? config.get("project_id").asText() : "";
        ProjectName projectName = ProjectName.of(projectId);
        return projectName;
    }
    public FixedCredentialsProvider getGCPCredentials() {
        try {
            String credentialsString = config.has("credentials_json") ? config.get("credentials_json").asText() : "";
            ServiceAccountCredentials credentials = null;
            credentials = ServiceAccountCredentials
                    .fromStream(new ByteArrayInputStream(credentialsString.getBytes(Charsets.UTF_8)));
            return FixedCredentialsProvider.create(credentials);
        } catch (Exception e) {
            logger.error("Unable to create credentials for gcp because", e);
            return null;
        }
    }

    public SubscriptionAdminClient getConsumer() {
        if (subscriptionAdminClient == null || subscriptionAdminClient.isTerminated()) {
            subscriptionAdminClient = getCheckConsumer();
        }
        return subscriptionAdminClient;
    }

    public SubscriptionAdminClient getCheckConsumer() {
        try {
            String credentialsString = config.has("credentials_json") ? config.get("credentials_json").asText() : "";
            ServiceAccountCredentials credentials = null;
            credentials = ServiceAccountCredentials
                    .fromStream(new ByteArrayInputStream(credentialsString.getBytes(Charsets.UTF_8)));
            SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(SubscriptionAdminSettings.newBuilder().setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build());
            return subscriptionAdminClient;
        } catch (Exception e) {
            logger.error("Unable to create google service account credentials for thread name {} and connector Id {}", consumerThreadName, connectorId, e);
        }
        return null;
    }

    public PullRequest getPullRequest() {
        final int maxNumberOfMessages  = config.has("max_number_of_messages") ? config.get("max_number_of_messages").asInt() : 100;
        PullRequest pullRequest = PullRequest.newBuilder()
                .setMaxMessages(maxNumberOfMessages)
                .setSubscription(subscription.toString())
                .build();
        return pullRequest;
    }

    public PullRequest getCheckPullRequest() {
        if (checkPullrequest == null) {
            int maxNumberOfMessages = 10;
            checkPullrequest = PullRequest.newBuilder()
                    .setMaxMessages(maxNumberOfMessages)
                    .setSubscription(subscription.toString())
                    .build();
        }
        return checkPullrequest;
    }

    public ProjectSubscriptionName getProjectSubscriptionName() {
        return subscription;
    }
}
