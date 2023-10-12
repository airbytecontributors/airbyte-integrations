/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.event.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.db.SqlDatabase;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.source.relationaldb.AbstractDbSource;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains helper functions and boilerplate for implementing a source connector for a
 * relational DB source.
 *
 * see io.airbyte.integrations.source.jdbc.AbstractJdbcSource if you are implementing a relational
 * DB which can be accessed via JDBC driver.
 */
public abstract class BicycleAbstractRelationalDbSource<DataType, Database extends SqlDatabase> extends
        AbstractDbSource<DataType, Database> implements Source {

  private static final Logger LOGGER = LoggerFactory.getLogger(BicycleAbstractRelationalDbSource.class);

  @Override
  public AutoCloseableIterator<JsonNode> queryTableFullRefresh(final Database database,
                                                               final List<String> columnNames,
                                                               final String schemaName,
                                                               final String tableName) {
    String sqlQuery = database.getSqlQuery();
    if (!StringUtils.isEmpty(sqlQuery)) {
      LOGGER.info("Queueing query for table: {} with SQL query {}", tableName, sqlQuery);
      return queryTable(database, sqlQuery);
    }
    LOGGER.info("Queueing query for table: {}", tableName);
    return queryTable(database, String.format("SELECT %s FROM %s limit 1000",
            enquoteIdentifierList(columnNames),
            getFullTableName(schemaName, tableName)));
  }

  protected String getIdentifierWithQuoting(final String identifier) {
    return getQuoteString() + identifier + getQuoteString();
  }

  protected String enquoteIdentifierList(final List<String> identifiers) {
    final StringJoiner joiner = new StringJoiner(",");
    for (final String identifier : identifiers) {
      joiner.add(getIdentifierWithQuoting(identifier));
    }
    return joiner.toString();
  }

  protected String getFullTableName(final String nameSpace, final String tableName) {
    return (nameSpace == null || nameSpace.isEmpty() ? getIdentifierWithQuoting(tableName)
        : getIdentifierWithQuoting(nameSpace) + "." + getIdentifierWithQuoting(tableName));
  }

  protected AutoCloseableIterator<JsonNode> queryTable(final Database database, final String sqlQuery) {
    return AutoCloseableIterators.lazyIterator(() -> {
      try {
        final Stream<JsonNode> stream = database.unsafeQuery(sqlQuery);
        return AutoCloseableIterators.fromStream(stream);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

}
