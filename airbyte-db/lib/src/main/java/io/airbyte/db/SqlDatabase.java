/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.stream.Stream;

public abstract class SqlDatabase extends AbstractDatabase {
  protected String sqlQuery;
  public String getSqlQuery() {
    return sqlQuery;
  }

  public abstract void execute(String sql) throws Exception;

  public abstract Stream<JsonNode> unsafeQuery(String sql, String... params) throws Exception;

}
