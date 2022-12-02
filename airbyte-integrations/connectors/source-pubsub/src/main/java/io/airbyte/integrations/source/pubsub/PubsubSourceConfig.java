package io.airbyte.integrations.source.pubsub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.common.base.Charsets;
import com.google.pubsub.v1.*;
import io.airbyte.integrations.base.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;

import static io.airbyte.integrations.source.pubsub.PubsubSource.STREAM_NAME;

public class PubsubSourceConfig {
    private static final Logger logger = LoggerFactory.getLogger(PubsubSourceConfig.class.getName());
    private String consumerThreadName;
    private final JsonNode config;
    private String connectorId;
    private SubscriptionAdminClient subscriptionAdminClient = null;
    private PullRequest checkPullrequest = null;
    private PullRequest pullRequest = null;
    private ProjectName projectName = null;
    private String subscriptionId = null;
    private String topicId = null;
    private boolean isBicycleSubscription = false;

    public PubsubSourceConfig(String consumerThreadName, final JsonNode config, String connectorId) {
        this.consumerThreadName = consumerThreadName;
        this.config = config;
        this.connectorId = connectorId;
        subscriptionAdminClient = getConsumer();
        subscriptionId = config.has("subscription_id") ? config.get("subscription_id").asText() : "";
        topicId = config.has("topic_name") ? config.get("topic_name").asText(): "";
    }

    public ProjectName getProjectName() {
        if (projectName == null) {
            final String projectId = config.has("project_id") ? config.get("project_id").asText() : "";
            projectName = ProjectName.of(projectId);
        }
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

    private String defaultSubscriptionId (String topic) {
        return topic + "-bicycle-sub-" + connectorId;
    }



    public String getOrCreateSubscriptionId() {
        if (subscriptionId.isBlank()) {
            subscriptionId = createSubscription();
        }
        return subscriptionId;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    private String createSubscription() {
        SubscriptionAdminClient consumer = getConsumer();
        String topic = config.has(STREAM_NAME) ? config.get(STREAM_NAME).asText() : "";
        int acknowledgeDeadline = config.has("acknowledge_deadline") ? config.get("acknowledge_deadline").asInt() : 10;
        TopicName topicName = TopicName.of(getProjectName().getProject(), topic);
        String subscriptionId = defaultSubscriptionId(topic);
        ProjectSubscriptionName projectSubscriptionName = getProjectSubscriptionName(subscriptionId);
        try {
            consumer.createSubscription(projectSubscriptionName.toString(), topicName, PushConfig.getDefaultInstance(), acknowledgeDeadline);
            isBicycleSubscription = true;
            return subscriptionId;
        } catch (AlreadyExistsException e) {
            logger.error("Failed to create the subscription because same name subscription already exists", e);
            logger.info("Using default subscription");
            return subscriptionId;
        } catch (Exception e) {
            logger.error("unable to create the subscription {}", subscriptionId, e);
            return "";
        }
    }

    public SubscriptionAdminClient getCheckConsumer() {
        try {
            String credentialsString = config.has("credentials_json") ? config.get("credentials_json").asText() : "";
            Integer maxInboundMessageSize = config.has("max_inbound_message_size") ? config.get("max_inbound_message_size").asInt() : 20971520;
            ServiceAccountCredentials credentials = null;
            credentials = ServiceAccountCredentials
                    .fromStream(new ByteArrayInputStream(credentialsString.getBytes(Charsets.UTF_8)));
            SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(SubscriptionAdminSettings.newBuilder()
                    .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                    .setTransportChannelProvider(SubscriptionAdminSettings
                            .defaultGrpcTransportProviderBuilder()
                            .setMaxInboundMessageSize(maxInboundMessageSize).build())
                    .build());
            return subscriptionAdminClient;
        } catch (Exception e) {
            logger.error("Unable to create google service account credentials for thread name {} and connector Id {}", consumerThreadName, connectorId, e);
        }
        return null;
    }

    public void deleteSubscription (String subscriptionId) {
        try {
            subscriptionAdminClient.deleteSubscription(ProjectSubscriptionName.of(projectName.getProject(), subscriptionId).toString());
        } catch (Exception e) {
            logger.error("Unable to delete Subscription {}", subscriptionId, e);
        }
    }

    public PullRequest getPullRequest(String subscriptionId) {
       if (pullRequest == null) {
           final int maxNumberOfMessages  = config.has("max_number_of_messages") ? config.get("max_number_of_messages").asInt() : 500;
           ProjectSubscriptionName projectSubscriptionName = getProjectSubscriptionName(subscriptionId);
           pullRequest = PullRequest.newBuilder()
                   .setMaxMessages(maxNumberOfMessages)
                   .setSubscription(projectSubscriptionName.toString())
                   .build();
       }
        return pullRequest;
    }

    public PullRequest getCheckPullRequest(String testSubscriptionId) {
        if (checkPullrequest == null) {
            ProjectSubscriptionName projectSubscriptionName = getProjectSubscriptionName(testSubscriptionId);
            int maxNumberOfMessages = 100;
            checkPullrequest = PullRequest.newBuilder()
                    .setMaxMessages(maxNumberOfMessages)
                    .setSubscription(projectSubscriptionName.toString())
                    .build();
        }
        return checkPullrequest;
    }

    public ProjectSubscriptionName getProjectSubscriptionName(String subscriptionId) {
        return ProjectSubscriptionName.of(getProjectName().getProject(), subscriptionId);
    }

    public boolean isBicycleSubscription() {
        return isBicycleSubscription;
    }
}
