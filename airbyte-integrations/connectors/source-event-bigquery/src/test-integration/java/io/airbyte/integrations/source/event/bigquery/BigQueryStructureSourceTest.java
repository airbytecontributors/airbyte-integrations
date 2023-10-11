/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.event.bigquery;

import static io.airbyte.integrations.source.event.bigquery.BigQuerySource.CONFIG_DATASET_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.inception.server.auth.api.SystemAuthenticator;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class BigQueryStructureSourceTest extends AbstractBigQuerySourceTest {

  @Override
  protected void createTable(String datasetId) throws SQLException {
    database.execute(
        "CREATE TABLE " + datasetId
            + ".id_and_name(id INT64, array_val ARRAY<STRUCT<key string, value STRUCT<string_val string>>>, object_val STRUCT<val_array ARRAY<STRUCT<value_str1 string>>, value_str2 string>);");
    database.execute(
        "INSERT INTO " + datasetId
            + ".id_and_name (id, array_val, object_val) VALUES "
            + "(1, [STRUCT('test1_1', STRUCT('struct1_1')), STRUCT('test1_2', STRUCT('struct1_2'))], STRUCT([STRUCT('value1_1'), STRUCT('value1_2')], 'test1_1')), "
            + "(2, [STRUCT('test2_1', STRUCT('struct2_1')), STRUCT('test2_2', STRUCT('struct2_2'))], STRUCT([STRUCT('value2_1'), STRUCT('value2_2')], 'test2_1')), "
            + "(3, [STRUCT('test3_1', STRUCT('struct3_1')), STRUCT('test3_2', STRUCT('struct3_2'))], STRUCT([STRUCT('value3_1'), STRUCT('value3_2')], 'test3_1'));");
  }

  protected ConfiguredAirbyteCatalog getConfiguredCatalog() {
    return CatalogHelpers.createConfiguredAirbyteCatalog(
        "id_and_name",
        config.get(CONFIG_DATASET_ID).asText(),
        Field.of("id", JsonSchemaType.NUMBER),
        Field.of("array_val", JsonSchemaType.ARRAY),
        Field.of("object_val", JsonSchemaType.OBJECT));
  }

  @Test
  public void testReadSuccess() throws Exception {

    BigQueryEventSource bigQueryEventSource = new BigQueryEventSource(Mockito.mock(SystemAuthenticator.class),
            Mockito.mock(EventConnectorJobStatusNotifier.class), null);

    bigQueryEventSource.read(config, getConfiguredCatalog(), null);

  //  final List<AirbyteMessage> actualMessages = MoreIterators.toList(new BigQuerySource().read(config, getConfiguredCatalog(), null));

  //  assertNotNull(actualMessages);
 //   assertEquals(3, actualMessages.size());
  }

}
