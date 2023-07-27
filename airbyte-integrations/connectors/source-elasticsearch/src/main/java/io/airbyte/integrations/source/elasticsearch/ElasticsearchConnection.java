/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.airbyte.integrations.source.elasticsearch.ElasticsearchConstants.*;


/**
 * All communication with Elasticsearch should be done through this class.
 */
public class ElasticsearchConnection {

    // this is the max number of hits we can query without paging
    private static final int MAX_HITS = 10000;
    private static final Logger log = LoggerFactory.getLogger(ElasticsearchConnection.class);
    private final RestHighLevelClient client;
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Creates a new ElasticsearchConnection that can be used to read/write records to indices
     *
     * @param config Configuration parameters for connecting to the Elasticsearch host
     */
    public ElasticsearchConnection(ConnectorConfiguration config) {
        log.info(String.format(
                "creating ElasticsearchConnection: %s", config.getEndpoint()));

        // Create the low-level client

        HttpHost httpHost = HttpHost.create(config.getEndpoint());

        RestClientBuilder builder = RestClient.builder(httpHost).setDefaultHeaders(configureHeaders(config)).setFailureListener((new FailureListener()));
        client = new RestHighLevelClient(builder);
    }

    static class FailureListener extends RestClient.FailureListener {

        @Override
        public void onFailure(Node node) {
            log.error("RestClient failure: {}", node);
        }

    }

    /**
     * Configures the default headers for requests to the Elasticsearch server
     *
     * @param config connection information
     * @return the default headers
     */
    protected Header[] configureHeaders(ConnectorConfiguration config) {
        final var headerList = new ArrayList<Header>();
        // add Authorization header if credentials are present
        final var auth = config.getAuthenticationMethod();
        switch (auth.getMethod()) {
            case secret -> {
                var bytes = (auth.getApiKeyId() + ":" + auth.getApiKeySecret()).getBytes(StandardCharsets.UTF_8);
                var header = "ApiKey " + Base64.getEncoder().encodeToString(bytes);
                headerList.add(new BasicHeader("Authorization", header));
            }
            case basic -> {
                var basicBytes = (auth.getUsername() + ":" + auth.getPassword()).getBytes(StandardCharsets.UTF_8);
                var basicHeader = "Basic " + Base64.getEncoder().encodeToString(basicBytes);
                headerList.add(new BasicHeader("Authorization", basicHeader));
            }
        }
        return headerList.toArray(new Header[headerList.size()]);
    }

    /**
     * Pings the Elasticsearch server for "up" check, and configuration validation
     * @return true if connection was successful
     */
    public boolean checkConnection() {
        log.info("checking elasticsearch connection");
        try {
            final var info = client.info(RequestOptions.DEFAULT);
            log.info("checked elasticsearch connection: {}, node-name: {}, version: {}", info.getClusterName(), info.getNodeName(), info.getVersion());
            return true;
        } catch (IOException e) {
            log.error("failed to ping elasticsearch", e);
            return false;
        } catch (Exception e) {
            log.error("unknown exception while pinging elasticsearch server", e);
            return false;
        }
    }


    /**
     * Shutdown the connection to the Elasticsearch server
     */
    public void close() throws IOException {
        this.client.close();
    }

    /**
     * Gets mappings (metadata for fields) from Elasticsearch cluster for given indices
     * @param indices A list of indices for which the mapping is required
     * @return String to MappingMetadata as a native Java Map
     * @throws IOException throws IOException if Elasticsearch request fails
     */
    public Map<String, MappingMetadata> getMappings(final List<String> indices) throws IOException {
        GetMappingsRequest request = new GetMappingsRequest();
        String[] copiedIndices = indices.toArray(String[]::new);
        request.indices(copiedIndices);
        GetMappingsResponse getMappingResponse = client.indices().getMapping(request, RequestOptions.DEFAULT);
        return getMappingResponse.mappings();
    }

    /**
     * Gets all mappings (metadata for fields) from Elasticsearch cluster
     * @return String to MappingMetadata as a native Java Map
     * @throws IOException throws IOException if Elasticsearch request fails
     */
    public Map<String, MappingMetadata> getAllMappings() throws IOException {
        // Need to exclude system mappings
        GetMappingsRequest request = new GetMappingsRequest();
        GetMappingsResponse getMappingResponse = client.indices().getMapping(request, RequestOptions.DEFAULT);
        return getMappingResponse.mappings();
    }

    /**
     * Returns a list of all records, without the metadata in JsonNode format
     * Uses scroll API for pagination
     * @param index index name in Elasticsearch cluster
     * @return list of documents
     * @throws IOException throws IOException if Elasticsearch request fails
     */
    public List<JsonNode> getRecords(final String index, final JsonNode timeRange) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // check performance
        searchSourceBuilder.size(MAX_HITS);

        if(timeRange!=null && timeRange.has("method") && timeRange.get("method").textValue().equals("custom")) {
            String timeField = timeRange.has(TIME_FIELD)? timeRange.get(TIME_FIELD).textValue(): ES_DEFAULT_TIME_FIELD;
            String from = timeRange.has(FROM)? timeRange.get(FROM).textValue() : null;
            String to = timeRange.has(TO)? timeRange.get(TO).textValue() : null;
            log.debug("Timefield {}, from {}, to {}", timeField, from, to);
            RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(timeField).from(from).to(to);
            searchSourceBuilder.query(rangeQueryBuilder);
        }
        else {
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        }

        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.scroll(scroll);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        log.debug("Running scroll query with scrollId {}", scrollId);
        List<JsonNode> data = new ArrayList<>();

        while (searchHits != null && searchHits.length > 0) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            if(data.size()>200000000){
                data.clear();
            }
            for (SearchHit hit : searchHits) {
                data.add(mapper.convertValue(hit, JsonNode.class).get("sourceAsMap"));
            }
            searchHits = searchResponse.getHits().getHits();
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        boolean succeeded = clearScrollResponse.isSucceeded();
        log.debug("Scroll response succeeded? {}", succeeded);
        log.info("{} RECORDS returned", data.size());
        return data;
    }


    /**
     * Returns a list of user defined indices, with system indices exclusions
     * made using regex variable ALL_INDICES_QUERY
     * @return indices list
     * @throws IOException throws IOException if Elasticsearch request fails
     */
    public List<String> userIndices() throws IOException {
        GetIndexRequest request = new GetIndexRequest(ElasticsearchConstants.ALL_INDICES_QUERY);
        GetIndexResponse response = this.client.indices().get(request, RequestOptions.DEFAULT);
        List<String> indices = Arrays.asList(response.getIndices());
        Pattern pattern = Pattern.compile(ElasticsearchConstants.REGEX_FOR_USER_INDICES_ONLY);
        indices = indices.stream().filter(pattern.asPredicate().negate()).toList();
        return indices;
    }

    /**
     * Returns a list of all indices including Elasticsearch system indices
     * @return indices list
     * @throws IOException throws IOException if Elasticsearch request fails
     */
    public List<String> allIndices() throws IOException {
        GetIndexRequest request = new GetIndexRequest(ElasticsearchConstants.ALL_INDICES_QUERY);
        GetIndexResponse response = this.client.indices().get(request, RequestOptions.DEFAULT);
        return Arrays.asList(response.getIndices());
    }

    public String getLatestTimestamp(String index, String fieldName) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(1);
        searchSourceBuilder.sort(new FieldSortBuilder(fieldName).order(SortOrder.DESC));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = this.client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        if(searchHits.length==0) {
            return new DateTime().toString();
        }
        else {
            Map<String, Object> map = searchHits[0].getSourceAsMap();
            return (String)map.get(fieldName);
        }
    }


}
