package io.airbyte.integrations.io.airbyte.integration_tests.sources;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.Databases;
import io.airbyte.db.jdbc.JdbcDatabase;
import org.testcontainers.containers.OracleContainer;

import java.sql.SQLException;

public class TestIt {

  private static OracleContainer container;
  private static JsonNode config;

  public static void main(String[] args) throws Exception {
    container = new OracleContainer("epiclabs/docker-oracle-xe-11g");
    container.start();

    config = Jsons.jsonNode(ImmutableMap.builder()
        .put("host", container.getHost())
        .put("port", container.getFirstMappedPort())
        .put("sid", container.getSid())
        .put("username", container.getUsername())
        .put("password", container.getPassword())
        .build());

    JdbcDatabase database = Databases.createJdbcDatabase(config.get("username").asText(),
        config.get("password").asText(),
        String.format("jdbc:oracle:thin:@%s:%s/%s",
            config.get("host").asText(),
            config.get("port").asText(),
            config.get("sid").asText()),
        "oracle.jdbc.driver.OracleDriver");

    database.execute(connection -> {
      connection.createStatement().execute("CREATE TABLE id_and_name(id INTEGER, name VARCHAR(200))");
      connection.createStatement().execute("INSERT INTO id_and_name (id, name) VALUES (1,'picard'),  (2, 'crusher'), (3, 'vash')");
      connection.createStatement().execute("CREATE TABLE starships(id INTEGER, name VARCHAR(200))");
      connection.createStatement().execute("INSERT INTO starships (id, name) VALUES (1,'enterprise-d'),  (2, 'defiant'), (3, 'yamato')");
    });

    database.close();
  }
}
