/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.failureinjectiontesting.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager.JDBCSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLSrcDataProvider {

  private static final Logger LOG = LoggerFactory.getLogger(MySQLSrcDataProvider.class);
  public static final String AUTHORS_TABLE = "Authors";
  public static final String BOOKS_TABLE = "Books";
  public static final HashMap<String, String> AUTHOR_TABLE_COLUMNS =
      new HashMap<>() {
        {
          put("author_id", "INT NOT NULL");
          put("name", "VARCHAR(200)");
        }
      };
  public static final HashMap<String, String> BOOK_TABLE_COLUMNS =
      new HashMap<>() {
        {
          put("author_id", "INT NOT NULL");
          put("book_id", "INT NOT NULL");
          put("name", "VARCHAR(200)");
        }
      };

  public static CloudSqlResourceManager createSourceResourceManagerWithSchema(String testName) {
    CloudSqlResourceManager cloudSqlResourceManager =
        CloudMySQLResourceManager.builder(testName).build();
    cloudSqlResourceManager.createTable(
        AUTHORS_TABLE, new JDBCSchema(AUTHOR_TABLE_COLUMNS, "author_id"));
    cloudSqlResourceManager.createTable(BOOKS_TABLE, new JDBCSchema(BOOK_TABLE_COLUMNS, "book_id"));
    return cloudSqlResourceManager;
  }

  public static boolean writeRowsInSourceDB(
      Integer startId, Integer endId, CloudSqlResourceManager sourceDBSqlResourceManager) {

    boolean success = true;
    List<Map<String, Object>> rows = new ArrayList<>();
    // Insert Authors
    for (int i = startId; i <= endId; i++) {
      Map<String, Object> values = new HashMap<>();
      values.put("author_id", i);
      values.put("name", "author_name_" + i);
      rows.add(values);
    }
    success &= sourceDBSqlResourceManager.write(AUTHORS_TABLE, rows);
    LOG.info(String.format("Wrote %d rows to table %s", rows.size(), AUTHORS_TABLE));

    rows = new ArrayList<>();
    if (success) {
      // Insert Books
      for (int i = startId; i <= endId; i++) {
        Map<String, Object> values = new HashMap<>();
        values.put("author_id", i);
        values.put("book_id", i);
        values.put("name", "book_name_" + i);
        rows.add(values);
      }
      success &= sourceDBSqlResourceManager.write(BOOKS_TABLE, rows);
      LOG.info(String.format("Wrote %d rows to table %s", rows.size(), BOOKS_TABLE));
    }

    return success;
  }
}
