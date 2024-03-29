/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.event.bigquery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class BicycleBigQueryWrapperTest {

  @Test
  public void testEmptyDatasetIdInConfig() throws IOException {
    final JsonNode configJson = Jsons.deserialize(MoreResources.readResource("test_config_empty_datasetid.json"));
    final JsonNode dbConfig = new BicycleBigQueryWrapper().toDatabaseConfig(configJson);
    assertTrue(dbConfig.get(BicycleBigQueryWrapper.CONFIG_DATASET_ID).isEmpty());
  }

  @Test
  public void testMissingDatasetIdInConfig() throws IOException {
    final JsonNode configJson = Jsons.deserialize(MoreResources.readResource("test_config_missing_datasetid.json"));
    final JsonNode dbConfig = new BicycleBigQueryWrapper().toDatabaseConfig(configJson);
    assertFalse(dbConfig.hasNonNull(BicycleBigQueryWrapper.CONFIG_DATASET_ID));
  }

  @Test
  public void testNullDatasetIdInConfig() throws IOException {
    final JsonNode configJson = Jsons.deserialize(MoreResources.readResource("test_config_null_datasetid.json"));
    final JsonNode dbConfig = new BicycleBigQueryWrapper().toDatabaseConfig(configJson);
    assertFalse(dbConfig.hasNonNull(BicycleBigQueryWrapper.CONFIG_DATASET_ID));
  }

  @Test
  public void testConfig() throws IOException {
    final JsonNode configJson = Jsons.deserialize(MoreResources.readResource("test_config.json"));
    final JsonNode dbConfig = new BicycleBigQueryWrapper().toDatabaseConfig(configJson);
    assertEquals("dataset", dbConfig.get(BicycleBigQueryWrapper.CONFIG_DATASET_ID).asText());
    assertEquals("project", dbConfig.get(BicycleBigQueryWrapper.CONFIG_PROJECT_ID).asText());
    assertEquals("credentials", dbConfig.get(BicycleBigQueryWrapper.CONFIG_CREDS).asText());
  }

}
