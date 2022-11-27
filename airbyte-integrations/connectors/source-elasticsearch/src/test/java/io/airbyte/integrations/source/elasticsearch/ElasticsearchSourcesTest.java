/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.elasticsearch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.integrations.source.elasticsearch.typemapper.ElasticsearchTypeMapper;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConnectorSpecification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

class ElasticsearchSourcesTest {
//  ObjectMapper mapper = new ObjectMapper();

  @Test
  @DisplayName("Spec should match")
  public void specShouldMatch() throws Exception {
    final ConnectorSpecification actual = new ElasticsearchSource(null,null).spec();
    final ConnectorSpecification expected = Jsons.deserialize(
        MoreResources.readResource("expected_spec.json"), ConnectorSpecification.class);
    assertEquals(expected, actual);
  }

  @Test
  @DisplayName("Actual mapper keyset should contain expected keyset")
  public void actualMapperKeySetShouldContainExpectedKeySet () {
    final Set<String> expectedKeySet = new HashSet<>(Arrays.asList(
            "binary", "boolean", "keyword", "constant_keyword",
            "wildcard", "long", "unsigned_long",
            "integer", "short", "byte", "double", "float",
            "half_float", "scaled_float", "date", "date_nanos", "ip",
            "text", "geo_point", "geo_shape", "shape", "point"));
    Set<String> actualKeySet = new HashSet<>(ElasticsearchTypeMapper.getTypeMapper().keySet());

    assertTrue(actualKeySet.containsAll(expectedKeySet));
  }

  @Test
  @DisplayName("Formatter should transform objects conforming to airbyte spec")
  public void testFormatter() throws IOException, UnsupportedDatatypeException {
    final JsonNode input = Jsons.deserialize(
            MoreResources.readResource("sample_input.json"), JsonNode.class);
    final JsonNode expectedOutput = Jsons.deserialize(
            MoreResources.readResource("expected_output.json"), JsonNode.class);
    JsonNode actualOutput = ElasticsearchTypeMapper.formatJSONSchema(input);
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  @DisplayName("Formatter should remove extra fields")
  public void testFormatterRemovals() throws IOException, UnsupportedDatatypeException {
    final JsonNode input = Jsons.deserialize(
            MoreResources.readResource("sample_input_extra_fields.json"), JsonNode.class);
    final JsonNode expectedOutput = Jsons.deserialize(
            MoreResources.readResource("expected_output_extra_fields.json"), JsonNode.class);
    JsonNode actualOutput = ElasticsearchTypeMapper.formatJSONSchema(input);
    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void t() throws Exception {
    ElasticsearchSource es = new ElasticsearchSource(null, null);
    final JsonNode config = Jsons.deserialize(MoreResources.readResource("config.json"), JsonNode.class);
    final ConfiguredAirbyteCatalog configuredAirbyteCatalog = Jsons.deserialize(MoreResources.readResource("configured_catalog.json"), ConfiguredAirbyteCatalog.class);
    es.read(config, configuredAirbyteCatalog, null);
  }


  @Test
  @DisplayName("Should return a list of cursor wise ascending data")
  public void testReturnAscendingList() throws Exception {
    final JsonNode config = Jsons.deserialize(MoreResources.readResource("config.json"), JsonNode.class);
    final ConnectorConfiguration configObject = new ObjectMapper().convertValue(config, ConnectorConfiguration.class);
    final ElasticsearchConnection connection = new ElasticsearchConnection(configObject);

    List<JsonNode> records = connection.getRecordsUsingCursor("kibana_sample_data_logs", "timestamp", null);
    for(JsonNode record: records) {
      System.out.println("timestamp::"+ record.get("timestamp"));
    }
    System.out.println(records.get(records.size()-1).get("timestamp").asText());
  }


  @Test
  @DisplayName("Should match state json schema")
  public void testStateJsonSchema() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode newState = (mapper.createObjectNode()).set("kibana_sample_data_logs",  mapper.createObjectNode().put("cursor", "timestamp").put("cursorField", "2023-01-05T21:45:26.749Z"));
    final JsonNode requiredState = Jsons.deserialize(MoreResources.readResource("state.json"), JsonNode.class);
    assertEquals(newState, requiredState);
  }


}
