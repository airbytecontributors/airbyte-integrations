package io.airbyte.integrations.source.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchConnector {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchConnector.class.getName());

    private static final String DEFAULT_USER = System.getProperty("ES_USER");
    private static final String DEFAULT_PASS = System.getProperty("ES_PASSWORD");
    public static final String DEFAULT_SCROLL_DURATION = "15m";
    public static final int DEFAULT_PAGE_SIZE = 1000;
    private static final int SECONDS = 1000;
    public static final int DEFAULT_SOCKET_TIMEOUT = 10 * SECONDS;
    public static final int DEFAULT_SEARCH_TIMEOUT = 30 * SECONDS;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private InMemoryConsumer inMemoryConsumer;

    public ElasticsearchConnector() {
    }

    public ElasticsearchConnector(InMemoryConsumer inMemoryConsumer) {
        this.inMemoryConsumer = inMemoryConsumer;
    }

    public List<JsonNode> getPreviewRecords(ConnectorConfiguration connectorConfiguration) {

        long dataLateness = connectorConfiguration.getDataLateness();
        long pollFrequency = connectorConfiguration.getPollFrequency();
        String queryLine = connectorConfiguration.getQueryWithIndexPattern();

        RestClientBuilder builder = createDefaultBuilder(connectorConfiguration);
        try (RestClient restClient = builder.build()) {
            testConnection(restClient);

            long now = System.currentTimeMillis();
            long startEpoch = now - dataLateness - pollFrequency;
            startEpoch -= startEpoch % pollFrequency;
            long endEpoch = startEpoch + pollFrequency;
            List<JsonNode> jsonNodes = search(restClient, startEpoch, endEpoch, queryLine, 100, true);
            return jsonNodes;
        } catch (Exception e) {
            LOG.error("Unable to get preview data for config " + connectorConfiguration, e);
        }

        return Collections.emptyList();
    }

    public void run() throws IOException, InterruptedException {
        RestClientBuilder builder = createDefaultBuilder(null);
        try (RestClient restClient = builder.build()) {
            testConnection(restClient);
            String queryLine = "_index:bigbasket-prod-k8s-* AND level:ERROR AND failed";
            long dataLateness = 180 * SECONDS;
            long pollFrequency = 60 * SECONDS;

            long now = System.currentTimeMillis();
            long startEpoch = now - dataLateness - pollFrequency;
            startEpoch -= startEpoch % pollFrequency;
            long endEpoch = startEpoch + pollFrequency;
            while (true) {
                LOG.info("Searching between {}:{}@{}", startEpoch, endEpoch,
                        (System.currentTimeMillis() - endEpoch) / SECONDS);
                search(restClient, startEpoch, endEpoch, queryLine);
                startEpoch = endEpoch;
                endEpoch = startEpoch + pollFrequency;
                while ((System.currentTimeMillis() - dataLateness) < endEpoch) {
                    //Added a while loop because sometimes the thread seems to be waking 2-3 seconds before time
                    long sleepTime = endEpoch - (System.currentTimeMillis() - dataLateness);
                    LOG.info("Sleeping: {}", sleepTime);
                    if (sleepTime > 0) {
                        Thread.sleep(sleepTime);
                    }
                }
            }
        }
    }

    public List<JsonNode> search(RestClient restClient, long startEpoch, long endEpoch, String queryLine,
                                 int pageSize, boolean isPreview) throws IOException {

        String scrollDuration = DEFAULT_SCROLL_DURATION;
        Request request = new Request("POST", "/_search?scroll=" + scrollDuration);
        String requestBody = getSearchRequest(startEpoch, endEpoch, queryLine, pageSize);
        StringEntity requestEntity = new StringEntity(requestBody);
        long currentPageSize = 0;
        String scrollId = null;
        List<JsonNode> previewJsonNodes = new ArrayList<>();
        try {
            do {
                List<JsonNode> jsonNodes = new ArrayList<>();
                JsonObject searchResponse = executeRequest(restClient, request, requestEntity);
                boolean timedOut = searchResponse.get("timed_out").getAsBoolean();
                LOG.info("timedOut = {}", timedOut);
                // TODO assert timedOut false
                scrollId = searchResponse.get("_scroll_id").getAsString();
                LOG.debug("scrollId = {}", scrollId);
                JsonObject shards = searchResponse.get("_shards").getAsJsonObject();
                int failedShardCount = shards.get("failed").getAsInt();
                LOG.debug("failedShardCount = {}", failedShardCount);
                // TODO assert failed == 0
                JsonObject hitsMeta = searchResponse.get("hits").getAsJsonObject();
                long numHits = hitsMeta.get("total").getAsLong();
                LOG.info("numHits = {}", numHits);
                JsonArray hits = hitsMeta.get("hits").getAsJsonArray();
                jsonNodes.addAll(convertHitsToJsonNodes(hits));
                if (isPreview) {
                    previewJsonNodes.addAll(jsonNodes);
                }
                if (isPreview && previewJsonNodes.size() >= 100) {
                    LOG.info("Received 100 records for preview");
                    break;
                } else if (!isPreview) {
                    inMemoryConsumer.addEventsToQueue(endEpoch, scrollId, jsonNodes);
                }
                currentPageSize = hits.size();
                LOG.info("Records size {}", jsonNodes.size());
                LOG.info("hits.size() = {}", currentPageSize);
                request = new Request("POST", "/_search/scroll");
                requestEntity = new StringEntity("{\n"
                        + "  \"scroll\" : \"" + scrollDuration + "\",\n"
                        + "  \"scroll_id\" : \"" + scrollId + "\" \n"
                        + "}");
            } while (currentPageSize > 0);
        } finally {
            if (scrollId != null) {
                request = new Request("DELETE", "/_search/scroll");
                requestEntity = new StringEntity("{\"scroll_id\" : \"" + scrollId + "\"}");
                JsonNode deleteResponse = executeRequestAsJsonNode(restClient, request, requestEntity);
                boolean succeeded = deleteResponse.get("succeeded").asBoolean();
                LOG.info("Delete scroll succeeded = {}", succeeded);
            }
        }

        return previewJsonNodes;

    }

    public List<JsonNode> search(RestClient restClient, long startEpoch, long endEpoch, String queryLine)
            throws IOException {

        return search(restClient, startEpoch, endEpoch, queryLine, DEFAULT_PAGE_SIZE, false);
    }

    private JsonNode executeRequestAsJsonNode(RestClient restClient, Request request, StringEntity requestEntity)
            throws IOException {
        if (requestEntity != null) {
            requestEntity.setContentType("application/json");
            request.setEntity(requestEntity);
        }
        Response response = restClient.performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();
        Header[] headers = response.getHeaders();
        String responseBody = EntityUtils.toString(response.getEntity());
        LOG.info("statusCode = {}", statusCode);
        // TODO assert status 200
        LOG.debug("responseBody = {}", responseBody);
        JsonNode jsonNode = objectMapper.readTree(responseBody);
        return jsonNode;
    }

    private JsonObject executeRequest(RestClient restClient, Request request, StringEntity requestEntity)
            throws IOException {
        if (requestEntity != null) {
            requestEntity.setContentType("application/json");
            request.setEntity(requestEntity);
        }
        Response response = restClient.performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();
        Header[] headers = response.getHeaders();
        String responseBody = EntityUtils.toString(response.getEntity());
        LOG.info("statusCode = {}", statusCode);
        // TODO assert status 200
        LOG.debug("responseBody = {}", responseBody);
        JsonObject jsonResponse = JsonParser.parseString(responseBody).getAsJsonObject();
        return jsonResponse;
    }

    private String getSearchRequest(long startEpoch, long endEpoch, String queryLine, int pageSize) {
        long searchTimeout = DEFAULT_SEARCH_TIMEOUT;
        String requestBody = "{\n"
                + "  \"version\": true,\n"
                + "  \"size\": " + pageSize + ",\n"
                + "  \"sort\": [\n"
                + "    {\n"
                + "      \"@timestamp\": {\n"
                + "        \"order\": \"asc\",\n"
                + "        \"unmapped_type\": \"boolean\"\n"
                + "      }\n"
                + "    }\n"
                + "  ],\n"
                + "  \"_source\": {\n"
                + "    \"excludes\": []\n"
                + "  },\n"
                + "  \"query\": {\n"
                + "    \"bool\": {\n"
                + "      \"must\": [\n"
                + "        {\n"
                + "          \"query_string\": {\n"
                + "            \"query\": \"" + queryLine + "\",\n"
                + "            \"analyze_wildcard\": true,\n"
                + "            \"default_field\": \"*\"\n"
                + "          }\n"
                + "        },\n"
                + "        {\n"
                + "          \"range\": {\n"
                + "            \"@timestamp\": {\n"
                + "              \"gte\": " + startEpoch + ",\n"
                + "              \"lt\": " + endEpoch + ",\n"
                + "              \"format\": \"epoch_millis\"\n"
                + "            }\n"
                + "          }\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  },\n"
                + "  \"timeout\": \"" + searchTimeout + "ms\"\n"
                + "}";
        LOG.info("Request Body {}", requestBody);
        return requestBody;
    }

    private RestClientBuilder createDefaultBuilder() {
        RestClientBuilder builder = RestClient.builder(getHttpHosts().toArray(new HttpHost[0]));
        List<Header> defaultHeaders = new ArrayList<>();
        if (DEFAULT_USER != null && DEFAULT_PASS != null) {
            String authHeader = "Basic " + getBasicAuthHeader(DEFAULT_USER, DEFAULT_PASS);
            defaultHeaders.add(new BasicHeader(HttpHeaders.AUTHORIZATION, authHeader));
        }
        builder.setDefaultHeaders(defaultHeaders.toArray(new Header[0]));
        builder.setRequestConfigCallback(rcBuilder -> rcBuilder.setSocketTimeout(DEFAULT_SOCKET_TIMEOUT));
        return builder;
    }

    public RestClientBuilder createDefaultBuilder(ConnectorConfiguration connectorConfiguration) {
        RestClientBuilder builder = RestClient.builder(getHttpHosts(connectorConfiguration).toArray(new HttpHost[0]));
        List<Header> defaultHeaders = new ArrayList<>();
        String userName = connectorConfiguration.getAuthenticationMethod().getUsername();
        String password = connectorConfiguration.getAuthenticationMethod().getPassword();
        if (userName != null && password != null) {
            String authHeader = "Basic " + getBasicAuthHeader(userName, password);
            defaultHeaders.add(new BasicHeader(HttpHeaders.AUTHORIZATION, authHeader));
        }

        builder.setDefaultHeaders(defaultHeaders.toArray(new Header[0]));
        builder.setRequestConfigCallback(rcBuilder -> rcBuilder.setSocketTimeout(DEFAULT_SOCKET_TIMEOUT));
        return builder;
    }

    public boolean testConnection(RestClient restClient) {
        Request request = new Request(
                "GET",
                "/_cluster/health");
        try {
            JsonObject healthResponse = executeRequest(restClient, request, null);
            LOG.info("cluster_name = {}", healthResponse.get("cluster_name"));
            // TODO assert cluster name not null
            if (healthResponse.get("cluster_name") == null) {
                return false;
            }
            LOG.info("status = {}", healthResponse.get("status"));
            if (healthResponse.get("status") == null) {
                return false;
            }
        }catch (Exception e) {
            LOG.error("Unable to test connection", e);
            return false;
        }

        return true;
        // TODO assert status not null
    }

    private List<HttpHost> getHttpHosts(ConnectorConfiguration connectorConfiguration) {
        List<HttpHost> hosts = new ArrayList<>();
        hosts.add(HttpHost.create(connectorConfiguration.getEndpoint()));
        return hosts;
    }

    private List<HttpHost> getHttpHosts() {
        List<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("bigbasket.ap-south-1.es.apptuit.ai", 443, "https"));
        return hosts;
    }

    private String getBasicAuthHeader(String user, String pass) {
        final String auth = user + ":" + pass;
        final byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.ISO_8859_1));
        return new String(encodedAuth, Charset.defaultCharset());
    }

    public List<JsonNode> convertHitsToJsonNodes(JsonArray hits) throws IOException {
        if (hits.size() < 1) {
            return Collections.EMPTY_LIST;
        }

        return toJsonNodes(hits);
    }

    private List<JsonNode> toJsonNodes(JsonArray hits) {

        List<JsonNode> jsonNodes = new ArrayList<>();
        for (JsonElement hit : hits) {
            try {
                StringWriter stringWriter = new StringWriter();
                JsonWriter jsonWriter = new JsonWriter(stringWriter);
                jsonWriter.setLenient(true);
                stringWriter.append("{\"_raw\":");
                Streams.write(hit, jsonWriter);
                stringWriter.append("}");
                jsonNodes.add(objectMapper.readTree(stringWriter.toString()));
            }catch (Exception e){
                LOG.error("Unable to deserialize json string ", e);
            }
        }

        return jsonNodes;
    }



}
