/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.event.snowflake;

import static io.airbyte.integrations.source.snowflake.SnowflakeDataSourceUtils.OAUTH_METHOD;
import static io.airbyte.integrations.source.snowflake.SnowflakeDataSourceUtils.UNRECOGNIZED;
import static io.airbyte.integrations.source.snowflake.SnowflakeDataSourceUtils.USERNAME_PASSWORD_METHOD;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.factory.DatabaseDriver;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.db.jdbc.StreamingJdbcDatabase;
import io.airbyte.db.jdbc.streaming.AdaptiveStreamingQueryConfig;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.source.event.bigquery.BigQueryEventSourceConfig;
import io.airbyte.integrations.source.event.snowflake.base.BicycleAbstractJdbcSource;
import io.airbyte.integrations.source.jdbc.AbstractJdbcSource;
import io.airbyte.integrations.source.snowflake.SnowflakeDataSourceUtils;
import io.airbyte.integrations.source.snowflake.SnowflakeSourceOperations;
import java.io.IOException;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BicycleSnowflakeWrapper extends BicycleAbstractJdbcSource<JDBCType> implements Source {

  private static final Logger LOGGER = LoggerFactory.getLogger(BicycleSnowflakeWrapper.class);
  public static final String DRIVER_CLASS = DatabaseDriver.SNOWFLAKE.getDriverClassName();
  public static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors.newScheduledThreadPool(1);


  public BicycleSnowflakeWrapper(){
    super(DRIVER_CLASS, AdaptiveStreamingQueryConfig::new, new SnowflakeSourceOperations());
  }

  public BicycleSnowflakeWrapper(BigQueryEventSourceConfig bigQueryEventSourceConfig) {
    super(DRIVER_CLASS, AdaptiveStreamingQueryConfig::new, new SnowflakeSourceOperations());
    this.bigQueryEventSourceConfig = bigQueryEventSourceConfig;
  }

  @Override
  public JdbcDatabase createDatabase(final JsonNode config) throws SQLException {
    final var dataSource = createDataSource(config);
    final var database = new StreamingJdbcDatabase(dataSource, new SnowflakeSourceOperations(), AdaptiveStreamingQueryConfig::new);
    quoteString = database.getMetaData().getIdentifierQuoteString();
    return database;
  }

  @Override
  protected DataSource createDataSource(final JsonNode config) {
    final DataSource dataSource = SnowflakeDataSourceUtils.createDataSource(config);
    dataSources.add(dataSource);
    return dataSource;
  }

  @Override
  public JsonNode toDatabaseConfig(final JsonNode config) {
    final String jdbcUrl = SnowflakeDataSourceUtils.buildJDBCUrl(config);

    if (config.has("credentials")) {
      final JsonNode credentials = config.get("credentials");
      final String authType =
          credentials.has("auth_type") ? credentials.get("auth_type").asText() : UNRECOGNIZED;
      return switch (authType) {
        case OAUTH_METHOD -> buildOAuthConfig(config, jdbcUrl);
        case USERNAME_PASSWORD_METHOD -> buildUsernamePasswordConfig(config.get("credentials"),
            jdbcUrl);
        default -> throw new IllegalArgumentException("Unrecognized auth type: " + authType);
      };
    } else {
      return buildUsernamePasswordConfig(config, jdbcUrl);
    }
  }

  @Override
  public Set<String> getExcludedInternalNameSpaces() {
    return Set.of(
        "INFORMATION_SCHEMA");
  }

  private JsonNode buildOAuthConfig(final JsonNode config, final String jdbcUrl) {
    final String accessToken;
    final var credentials = config.get("credentials");
    try {
      accessToken = SnowflakeDataSourceUtils.getAccessTokenUsingRefreshToken(
          config.get("host").asText(), credentials.get("client_id").asText(),
          credentials.get("client_secret").asText(), credentials.get("refresh_token").asText());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    final ImmutableMap.Builder<Object, Object> configBuilder = ImmutableMap.builder()
        .put("connection_properties",
            String.join(";", "authenticator=oauth", "token=" + accessToken))
        .put("jdbc_url", jdbcUrl);
    return Jsons.jsonNode(configBuilder.build());
  }

  private JsonNode buildUsernamePasswordConfig(final JsonNode config, final String jdbcUrl) {
    final ImmutableMap.Builder<Object, Object> configBuilder = ImmutableMap.builder()
        .put("username", config.get("username").asText())
        .put("password", config.get("password").asText())
        .put("jdbc_url", jdbcUrl);
    LOGGER.info(jdbcUrl);
    return Jsons.jsonNode(configBuilder.build());
  }

}
