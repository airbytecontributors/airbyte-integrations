/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.event.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.functional.CheckedConsumer;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.db.SqlDatabase;
import io.airbyte.db.bigquery.BigQueryDatabase;
import io.airbyte.db.bigquery.BigQuerySourceOperations;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.source.relationaldb.TableInfo;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.CommonField;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.JsonSchemaType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BicycleBigQueryWrapper extends BicycleAbstractRelationalDbSource<StandardSQLTypeName, BigQueryDatabase> implements Source {

  private static final Logger LOGGER = LoggerFactory.getLogger(BicycleBigQueryWrapper.class);
  private static final String QUOTE = "`";

  public static final String CONFIG_DATASET_ID = "dataset_id";
  public static final String CONFIG_PROJECT_ID = "project_id";
  public static final String CONFIG_CREDS = "credentials_json";
  public static final String SQL_QUERY = "sql_query";
  public static final String DATA_FORMATTER_TYPE = "data_format_type";
  public static final String FORMAT_TYPE = "format_type";
  public static final String UNMAP_COLUMNS_NAME = "unmap_columns_name";
  public static final String MATCH_STREAMS_NAME = "match_streams_name";
  public static final String SYNC_MODE = "syncMode";
  public static final String SYNC_TYPE = "syncType";
  public static final String FULL_SYNC_TYPE = "Full Sync";
  public static final String INCREMENTAL_SYNC_TYPE = "Incremental Sync";
  public static final String CURSOR_FIELD = "cursorField";
  public static final String CUSTOM_DIMENSION_MAPPING = "custom_dimension_mapping";
  public static final String CUSTOM_DIMENSION_MAPPING_COLUMN_NAME = "custom_dimension_column_name";
  public static final String CUSTOM_DIMENSION_MAPPING_COLUMN_NAME_DEFAULT = "customDimensions";

  private JsonNode dbConfig;
  private final BigQuerySourceOperations sourceOperations = new BigQuerySourceOperations();

  public BicycleBigQueryWrapper() {
  }

  public BicycleBigQueryWrapper(BigQueryEventSourceConfig bigQueryEventSourceConfig) {
    this.bigQueryEventSourceConfig = bigQueryEventSourceConfig;
  }

  @Override
  public JsonNode toDatabaseConfig(final JsonNode config) {
    final var conf = ImmutableMap.builder()
        .put(CONFIG_PROJECT_ID, config.get(CONFIG_PROJECT_ID).asText())
        .put(CONFIG_CREDS, config.get(CONFIG_CREDS).asText());
    if (config.hasNonNull(CONFIG_DATASET_ID)) {
      conf.put(CONFIG_DATASET_ID, config.get(CONFIG_DATASET_ID).asText());
    }
    return Jsons.jsonNode(conf.build());
  }

  @Override
  protected BigQueryDatabase createDatabase(final JsonNode config) {
    dbConfig = Jsons.clone(config);
    String sqlQuery = config.has(SQL_QUERY) ? config.get(SQL_QUERY).asText() : null;
    return new BigQueryDatabase(config.get(CONFIG_PROJECT_ID).asText(),
            config.get(CONFIG_CREDS).asText(), sqlQuery);
  }

  @Override
  public List<CheckedConsumer<BigQueryDatabase, Exception>> getCheckOperations(final JsonNode config) {
    final List<CheckedConsumer<BigQueryDatabase, Exception>> checkList = new ArrayList<>();
    checkList.add(database -> {
      if (database.query("select 1").count() < 1)
        throw new Exception("Unable to execute any query on the source!");
      else
        LOGGER.info("The source passed the basic query test!");
    });

    checkList.add(database -> {
      if (isDatasetConfigured(database)) {
        database.query(String.format("select 1 from %s where 1=0",
            getFullTableName(getConfigDatasetId(database), "INFORMATION_SCHEMA.TABLES")));
        LOGGER.info("The source passed the Dataset query test!");
      } else {
        LOGGER.info("The Dataset query test is skipped due to not configured datasetId!");
      }
    });

    return checkList;
  }

  @Override
  protected JsonSchemaType getType(final StandardSQLTypeName columnType) {
    return sourceOperations.getJsonType(columnType);
  }

  @Override
  public Set<String> getExcludedInternalNameSpaces() {
    return Collections.emptySet();
  }

  @Override
  protected List<TableInfo<CommonField<StandardSQLTypeName>>> discoverInternal(final BigQueryDatabase database) throws Exception {
    return discoverInternal(database, null);
  }

  @Override
  protected List<TableInfo<CommonField<StandardSQLTypeName>>> discoverInternal(final BigQueryDatabase database, final String schema) {
    final String projectId = dbConfig.get(CONFIG_PROJECT_ID).asText();
    final List<Table> tables =
        (isDatasetConfigured(database) ? database.getDatasetTables(getConfigDatasetId(database)) : database.getProjectTables(projectId));
    final List<TableInfo<CommonField<StandardSQLTypeName>>> result = new ArrayList<>();
    tables.stream().map(table -> TableInfo.<CommonField<StandardSQLTypeName>>builder()
        .nameSpace(table.getTableId().getDataset())
        .name(table.getTableId().getTable())
        .fields(Objects.requireNonNull(table.getDefinition().getSchema()).getFields().stream()
            .map(f -> {
              final StandardSQLTypeName standardType = f.getType().getStandardType();
              return new CommonField<>(f.getName(), standardType);
            })
            .collect(Collectors.toList()))
        .build())
        .forEach(result::add);
    return result;
  }

  @Override
  protected Map<String, List<String>> discoverPrimaryKeys(final BigQueryDatabase database,
                                                          final List<TableInfo<CommonField<StandardSQLTypeName>>> tableInfos) {
    return Collections.emptyMap();
  }

  @Override
  protected String getQuoteString() {
    return QUOTE;
  }

  @Override
  public AutoCloseableIterator<AirbyteMessage> read(JsonNode config, ConfiguredAirbyteCatalog catalog,
                                                    JsonNode state) throws Exception {
    return super.read(config, catalog, state);
  }

  @Override
  public AutoCloseableIterator<JsonNode> queryTableIncremental(final BigQueryDatabase database,
                                                               final List<String> columnNames,
                                                               final String schemaName,
                                                               final String tableName,
                                                               final String cursorField,
                                                               final StandardSQLTypeName cursorFieldType,
                                                               final String cursor) {
    String sqlQuery = database.getSqlQuery();
    if (!StringUtils.isEmpty(sqlQuery)) {
      try {
        LOGGER.info("Queueing incremental query for table: {} with SQL query {}", tableName, sqlQuery);
        Select selectStatement = (Select) CCJSqlParserUtil.parse(sqlQuery);
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        Expression where = plainSelect.getWhere();
        String extraWhereClause = " AND " + cursorField + " > ?";
        if (where != null) {
          where = CCJSqlParserUtil.parseCondExpression(where.toString() + extraWhereClause);
        } else {
          where = CCJSqlParserUtil.parseCondExpression(extraWhereClause.substring(5));
        }
        plainSelect.setWhere(where);
        String newQuery = selectStatement.toString();
        return queryTableWithParams(database, newQuery,
                sourceOperations.getQueryParameter(cursorFieldType, cursor));
      } catch (JSQLParserException e) {
        LOGGER.error("Unable to parse Sql query");
      }
    }
    return queryTableWithParams(database, String.format("SELECT %s FROM %s WHERE %s > ? ORDER BY %s limit %d",
        enquoteIdentifierList(columnNames),
        getFullTableName(schemaName, tableName),
        cursorField, cursorField, bigQueryEventSourceConfig.getDefaultLimit()),
        sourceOperations.getQueryParameter(cursorFieldType, cursor));
  }

  private AutoCloseableIterator<JsonNode> queryTableWithParams(final BigQueryDatabase database,
                                                               final String sqlQuery,
                                                               final QueryParameterValue... params) {
    return AutoCloseableIterators.lazyIterator(() -> {
      try {
        final Stream<JsonNode> stream = database.query(sqlQuery, params);
        return AutoCloseableIterators.fromStream(stream);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private boolean isDatasetConfigured(final SqlDatabase database) {
    final JsonNode config = database.getSourceConfig();
    return config.hasNonNull(CONFIG_DATASET_ID) ? !config.get(CONFIG_DATASET_ID).asText().isEmpty() : false;
  }

  private String getConfigDatasetId(final SqlDatabase database) {
    return (isDatasetConfigured(database) ? database.getSourceConfig().get(CONFIG_DATASET_ID).asText() : "");
  }

  public static void main(final String[] args) throws Exception {
    final Source source = new BicycleBigQueryWrapper();
    LOGGER.info("starting source: {}", BicycleBigQueryWrapper.class);
    new IntegrationRunner(source).run(args);
    LOGGER.info("completed source: {}", BicycleBigQueryWrapper.class);
  }

  @Override
  public void close() throws Exception {}

}
