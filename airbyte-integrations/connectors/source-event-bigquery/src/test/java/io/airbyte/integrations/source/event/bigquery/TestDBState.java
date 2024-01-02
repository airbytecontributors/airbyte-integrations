package io.airbyte.integrations.source.event.bigquery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.integrations.source.relationaldb.models.DbState;
import static io.airbyte.integrations.source.event.bigquery.BigQueryLiveTest.readFileAsString;

/**
 * @author sumitmaheshwari
 * Created on 29/12/2023
 */
public class TestDBState {

  public static void main(String[] args) throws JsonProcessingException {

    String configString = readFileAsString("dbstate.json");

    ObjectMapper objectMapper = new ObjectMapper();

    DbState dbState = objectMapper.readValue(configString, DbState.class);
    System.out.println(dbState);

  }

}
