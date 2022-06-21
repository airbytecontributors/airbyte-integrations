package io.airbyte.integrations.source.elasticsearch.typemapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.integrations.source.elasticsearch.UnsupportedDatatypeException;

import java.util.*;

import static io.airbyte.integrations.source.elasticsearch.ElasticsearchInclusions.KEEP_LIST;
import static java.util.Map.entry;

public class ElasticsearchTypeMapper {
    private static final ObjectMapper mapper = new ObjectMapper();

    /*
    Mapping from elasticsearch to Airbyte types
    Elasticsearch data types: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
    Airbyte data types: https://docs.airbyte.com/understanding-airbyte/supported-data-types/

    In Elasticsearch, there is no dedicated array data type.
    Any field can contain zero or more values by default, however,
    all values in the array must be of the same data type
    */
    private static final Map<String, Object> ElasticSearchToAirbyte = Map.ofEntries(
            // BINARY
            entry("binary", Arrays.asList("string", "array")),

            // BOOLEAN
            entry("boolean", Arrays.asList("boolean", "array")),

            // KEYWORD FAMILY
            entry("keyword", Arrays.asList("string", "array", "number", "integer")),
            entry("constant_keyword",  Arrays.asList("string", "array", "number", "integer")),
            entry("wildcard",  Arrays.asList("string", "array", "number", "integer")),

            // NUMBERS
            entry("long", Arrays.asList("integer", "array")),
            entry("unsigned_long", Arrays.asList("integer", "array")),
            entry("integer", Arrays.asList("integer", "array")),
            entry("short", Arrays.asList("integer", "array")),
            entry("byte", Arrays.asList("integer", "array")),
            entry("double", Arrays.asList("number", "array")),
            entry("float", Arrays.asList("number", "array")),
            entry("half_float", Arrays.asList("number", "array")),
            entry("scaled_float", Arrays.asList("number", "array")),

            // ALIAS
            /* Writes to alias field not supported by ES. Can be safely ignored */

            // DATES
            entry("date", Arrays.asList("string", "array")),
            entry("date_nanos", Arrays.asList("number", "array")),

            // OBJECTS AND RELATIONAL TYPES
            entry("object", Arrays.asList("object", "array")),
            entry("flattened", Arrays.asList("object", "array")),
            entry("nested", Arrays.asList("object", "string")),
            entry("join", Arrays.asList("object", "string")),

            // STRUCTURED DATA TYPES
            entry("integer_range", Arrays.asList("object", "array")),
            entry("float_range", Arrays.asList("object", "array")),
            entry("long_range", Arrays.asList("object", "array")),
            entry("double_range", Arrays.asList("object", "array")),
            entry("date_range", Arrays.asList("object", "array")),
            entry("ip_range", Arrays.asList("object", "array")),
            entry("ip", Arrays.asList("string", "array")),
            entry("version", Arrays.asList("string", "array")),
            entry("murmur3", Arrays.asList("object", "array")),

            // AGGREGATE METRIC FIELD TYPES
            entry("aggregate_metric_double", Arrays.asList("object", "array")),
            entry("histogram", Arrays.asList("object", "array")),

            // TEXT SEARCH TYPES
            entry("text", Arrays.asList("string", "array")),
            entry("alias", Arrays.asList("string", "array")),
            entry("search_as_you_type", Arrays.asList("string", "array")),
            entry("token_count", Arrays.asList("integer", "array")),

            // DOCUMENT RANKING
            entry("dense_vector", "array"),
        //        entry("rank_feature", "integer"), THEY ARE PUTTING OBJECTS HERE AS WELL????

            // SPATIAL DATA TYPES (HARD TO HANDLE AS QUERYING MECHANISM IS BASED ON SHAPE, which has multiple fields)
            entry("geo_point", Arrays.asList("object", "array")),
            entry("geo_shape", Arrays.asList("object", "array")),
            entry("shape", Arrays.asList("object", "array")),
            entry("point", Arrays.asList("object", "array"))

    );

    public static Map<String, Object> getTypeMapper() {
        return ElasticSearchToAirbyte;
    }


    /**
     * @param node JsonNode node which we want to format
     * @return JsonNode
     * @throws UnsupportedDatatypeException throws an exception if none of the types match
     */
    public static JsonNode formatJSONSchema(JsonNode node) throws UnsupportedDatatypeException {
        if(node.isObject()) {
            if(!node.has("type") || node.has("properties")) {
                ((ObjectNode)node).put("type", "object");
            }
            else if(node.has("type") && node.get("type").getNodeType()== JsonNodeType.STRING) {
                retainAirbyteFieldsOnly(node);

                final String nodeType = node.get("type").textValue();

                if (ElasticSearchToAirbyte.containsKey(nodeType)) {
                    ((ObjectNode) node).remove("type");
                    ((ObjectNode) node).set("type", mapper.valueToTree(ElasticSearchToAirbyte.get(nodeType)));
                }
                else throw new UnsupportedDatatypeException("Cannot map unsupported data type to Airbyte data type: "+node.get("type").textValue());
            }
            node.fields().forEachRemaining(entry -> {
                try {
                    formatJSONSchema(entry.getValue());
                } catch (UnsupportedDatatypeException e) {
                    throw new RuntimeException(e);
                }
            });
            if(node.path("properties").path("type").getNodeType() == JsonNodeType.STRING) {
                ((ObjectNode)node.path("properties")).remove("type");
            }
            else if(node.has("properties")) {
                ((ObjectNode)node).set("type", mapper.valueToTree(Arrays.asList("array", "object")));
            }
        }
        else if(node.isArray()) {
            ArrayNode arrayNode = (ArrayNode) node;
            Iterator<JsonNode> temp = arrayNode.elements();
            while (temp.hasNext()) {
                formatJSONSchema(temp.next());
            }
        }
        return node;
    }

    private static void retainAirbyteFieldsOnly(JsonNode jsonNode) {
        if(jsonNode instanceof ObjectNode) {
            ((ObjectNode)jsonNode).retain(KEEP_LIST);
        }
    }


}
