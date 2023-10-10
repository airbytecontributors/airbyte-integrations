/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.event.bigquery;

import static io.airbyte.integrations.source.event.bigquery.BigQuerySource.CONFIG_CREDS;
import static io.airbyte.integrations.source.event.bigquery.BigQuerySource.CONFIG_DATASET_ID;
import static io.airbyte.integrations.source.event.bigquery.BigQuerySource.CONFIG_PROJECT_ID;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.string.Strings;
import io.airbyte.db.bigquery.BigQueryDatabase;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

abstract class AbstractBigQuerySourceTest {

  private static final Path CREDENTIALS_PATH = Path.of("secrets/credentials.json");

  protected BigQueryDatabase database;
  protected Dataset dataset;
  protected JsonNode config;

  @BeforeEach
  void setUp() throws IOException, SQLException {
    if (!Files.exists(CREDENTIALS_PATH)) {
      throw new IllegalStateException(
          "Must provide path to a big query credentials file. By default {module-root}/" + CREDENTIALS_PATH
              + ". Override by setting setting path with the CREDENTIALS_PATH constant.");
    }

    final String credentialsJsonString = Files.readString(CREDENTIALS_PATH);
   // final String credentialsJsonString = "{   \"type\": \"service_account\",   \"project_id\": \"piyush-connector-test\",   \"private_key_id\": \"94982aaa5548c6fd7545c2707223960094c1daf1\",   \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDWrqt1Ikf0BgPg\\nfconM4RuloYvIxmuDTiyss+WWLFD6CPbyBIaPNcMWILHQRsWN0C4EutYM5rVm/JW\\nj1cNnEBGtXqxKXL/pFnQVxeNMp32rU/6RFtwF1gAevaSHpax+23+Qt8wgjWJ/jdg\\nypdD+ITLEA+jSDL0aI2jwNvQWUAIPtMfanIELQV2W3v5XcuOUmzu14GqKHkngxbj\\nqzk3GTVH3ChaiEnDIJe5n48YTLqYravzj6GXveTzdf8NfanWvGPlnZ3jVH4n7sh8\\naHNDqY/u4zLcxwV1w5CaOh3wFHTI/qpPoyr708X6shNda+X5nWRsNPMi69hwAgpU\\nr28DjfdVAgMBAAECggEADvJYn/jz3y1Us2+z6uMfKx88wWcQCyUC17eI/2k2Z57F\\nHFtYyKMyv7p2hHk9HGCwF1YYikAG3YIULxWfac8BT0r5pgBXmtfY+Z85VbclS1Q0\\nDMhvQKXHa+Ujv0xKO/hgPJhwTJghh8rUv97rBYGge+N8Nec5cPh0irxLMZLSg/Wn\\nC2pclVgQSxk/rVPvR4I2LV09Xki1pkbP1aHG1UQXtIsEPpFODkazhT8wJGUB6DSP\\nN60Zwa5Q0T58P55HCcfKpI0AIzLIHdV4GOC0J8IjntQv5YmUuCbb3G6gzVHoCMYD\\n6jpVfFIB3LM/IEPC7Gm3kx7BO7NgBTgFYiAYTIXt0QKBgQD+r5BMNNEPvoxmpQxI\\nI+o+eRFE2wBJIOdAufaqZ9AUiIX1EyXA7f5ez4XZQfuUm49E4MOHmtJ98TQ4mRmQ\\nWwLuD/Dau3aXvofmh3/DBufiBzmImmEAAv5juBuXazF/l9+dVScrH5P69+L2wAgc\\nwIeagVx0An+W24njIjLrG1TLaQKBgQDXykMVbzIM+VZ4FuU0fN29d/xgKec7A5AA\\nXeLzH7n1Kh7+vZ13Zdyw471NKvGX2i0gE9E/kBbr8CVbsuQdOG0WGXhlX6PjVCum\\nAdfTre7QSkA67viN7jnYkYY5K9Xf6vVmhyLfjbrebjeGRSkuwTn/BV/b+9YFdSkA\\nUTb3p80rDQKBgGejr9ejApQimWXHsjjFDLSBdcqMSBB6KEDJrBvqBN3mNITnVsys\\nzH9ueWxQkn0F1IZV5JuRICkJ95M0kheRQZ74s61+7aKQcSMZbp9BdykegYYaNHDZ\\nxwVzTR8QCuN+PQv8M+Uzg3d0WlLJEEQLAOOZ0rKOtbcA/ZqFNy0/QQFJAoGBAItw\\nsuqt4OzLThnpyZCiQ3NEjnvhd/8FbU8fXCD41mRbJI4+Hvvhgbt4XoH4Bwe2P0Sk\\nKanmRTmPA8T+kzNim/MU4Fy5caDbah0qwbmSgmhsIpcyhsDIOO7+EnrtHZZFMPBg\\n0KMe5asIE68bD+KbkAuAAJKZaJI8RBNlZ5ye/RAdAoGAIB5G4HZL/2PvFUEitrV5\\nmilqfYHS7GQhTWpuqeLcdUxixj6cyfKJnzieUOB41gPfLxzOPTNqYe60sJ0aPk29\\nUx98svrxxCQCHJNwwjffxii/UPPhUxGFOh3ETRGhHJImnLsnRxsjsI95z0DX3GQl\\n/Mncbnxg6ihGiMQts3gmoBU=\\n-----END PRIVATE KEY-----\\n\",   \"client_email\": \"bigquery-dev-service-account@piyush-connector-test.iam.gserviceaccount.com\",   \"client_id\": \"105613528114554791184\",   \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",   \"token_uri\": \"https://oauth2.googleapis.com/token\",   \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",   \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/bigquery-dev-service-account%40piyush-connector-test.iam.gserviceaccount.com\" }";

    final JsonNode credentialsJson = Jsons.deserialize(credentialsJsonString);
    final String projectId = credentialsJson.get(CONFIG_PROJECT_ID).asText();
    final String datasetLocation = "US";

    final String datasetId = Strings.addRandomSuffix("airbyte_tests", "_", 8);

    config = Jsons.jsonNode(ImmutableMap.builder()
        .put(CONFIG_PROJECT_ID, projectId)
        .put(CONFIG_CREDS, credentialsJsonString)
        .put(CONFIG_DATASET_ID, datasetId)
        .build());

    database = new BigQueryDatabase(config.get(CONFIG_PROJECT_ID).asText(), credentialsJsonString);

    final DatasetInfo datasetInfo =
        DatasetInfo.newBuilder(config.get(CONFIG_DATASET_ID).asText()).setLocation(datasetLocation).build();
    dataset = database.getBigQuery().create(datasetInfo);

    createTable(datasetId);
  }

  @AfterEach
  void tearDown() {
    database.cleanDataSet(dataset.getDatasetId().getDataset());
  }

  protected abstract void createTable(String datasetId) throws SQLException;

  protected abstract ConfiguredAirbyteCatalog getConfiguredCatalog();

}
