package io.airbyte.integrations.source.relationaldb;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.function.Function;

@FunctionalInterface
public interface DatabaseConfigMapper extends Function<JsonNode, JsonNode> {}
