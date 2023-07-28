/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.elasticsearch;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;
import org.apache.commons.lang3.StringEscapeUtils;

import static io.airbyte.integrations.source.elasticsearch.ElasticsearchConnector.DEFAULT_PAGE_SIZE;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectorConfiguration {

    private String endpoint;
    private boolean upsert;
    private Integer pageSize;
    private long pollFrequency;
    private long dataLateness;
    private String indexPattern;
    private String query;
    private String timestampField;

    private AuthenticationMethod authenticationMethod = new AuthenticationMethod();

    public ConnectorConfiguration() {
    }

    public static ConnectorConfiguration fromJsonNode(JsonNode config) {
        return new ObjectMapper().convertValue(config, ConnectorConfiguration.class);
    }

    public String getEndpoint() {
        if (this.endpoint != null && this.endpoint.endsWith("/")) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }
        return this.endpoint;
    }

    public boolean isUpsert() {
        return this.upsert;
    }

    public AuthenticationMethod getAuthenticationMethod() {
        return this.authenticationMethod;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setUpsert(boolean upsert) {
        this.upsert = upsert;
    }

    public void setAuthenticationMethod(AuthenticationMethod authenticationMethod) {
        this.authenticationMethod = authenticationMethod;
    }

    public long getPollFrequency() {
        return pollFrequency * 1000;
    }

    public int getPageSize() {
        return pageSize == null ? DEFAULT_PAGE_SIZE : pageSize;
    }


    public long getDataLateness() {
        return dataLateness * 1000;
    }

    public String getIndexPattern() {
        return indexPattern;
    }

    public String getQuery() {
        return query;
    }

    public String getTimestampField() {
        return timestampField;
    }

    public String getQueryWithIndexPattern() {
        String queryWithIndex =  "_index:" + indexPattern + " AND " + query;
        return escapeString(queryWithIndex);
    }

    public String escapeString(String str) {
        return StringEscapeUtils.escapeJava(str);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConnectorConfiguration that = (ConnectorConfiguration) o;
        return upsert == that.upsert && Objects.equals(endpoint, that.endpoint) &&
                Objects.equals(authenticationMethod, that.authenticationMethod);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoint, upsert, authenticationMethod);
    }

    @Override
    public String toString() {
        return "ConnectorConfiguration{" +
                "endpoint='" + endpoint + '\'' +
                ", upsert=" + upsert +
                ", pollFrequency=" + pollFrequency +
                ", dataLateness=" + dataLateness +
                ", indexPattern='" + indexPattern + '\'' +
                ", query='" + query + '\'' +
                ", timestampField='" + timestampField + '\'' +
                ", authenticationMethod=" + authenticationMethod +
                '}';
    }

    static class AuthenticationMethod {

        private ElasticsearchAuthenticationMethod method = ElasticsearchAuthenticationMethod.none;
        private String username;
        private String password;
        private String apiKeyId;
        private String apiKeySecret;

        public ElasticsearchAuthenticationMethod getMethod() {
            return this.method;
        }

        public String getUsername() {
            return this.username;
        }

        public String getPassword() {
            return this.password;
        }

        public String getApiKeyId() {
            return this.apiKeyId;
        }

        public String getApiKeySecret() {
            return this.apiKeySecret;
        }

        public void setMethod(ElasticsearchAuthenticationMethod method) {
            this.method = method;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public void setApiKeyId(String apiKeyId) {
            this.apiKeyId = apiKeyId;
        }

        public void setApiKeySecret(String apiKeySecret) {
            this.apiKeySecret = apiKeySecret;
        }

        public boolean isValid() {
            return switch (this.method) {
                case none -> true;
                case basic -> Objects.nonNull(this.username) && Objects.nonNull(this.password);
                case secret -> Objects.nonNull(this.apiKeyId) && Objects.nonNull(this.apiKeySecret);
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AuthenticationMethod that = (AuthenticationMethod) o;
            return method == that.method &&
                    Objects.equals(username, that.username) &&
                    Objects.equals(password, that.password) &&
                    Objects.equals(apiKeyId, that.apiKeyId) &&
                    Objects.equals(apiKeySecret, that.apiKeySecret);
        }

        @Override
        public int hashCode() {
            return Objects.hash(method, username, password, apiKeyId, apiKeySecret);
        }

        @Override
        public String toString() {
            return "AuthenticationMethod{" +
                    "method=" + method +
                    ", username='" + username + '\'' +
                    ", apiKeyId='" + apiKeyId + '\'' +
                    '}';
        }


    }
}
