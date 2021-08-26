/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.bigtable;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * This helper class allows fetching a sorted map containing the columns that make up the (compound)
 * primary key in a @see <a href="http://cassandra.apache.org/">Apache Cassandra</a> table. See
 * method documentation for futher details.
 */
class CassandraKeyUtils {

  private static final String COLUMN_NAME_COLUMN = "column_name";
  private static final String KIND_COLUMN = "kind";
  private static final String POSITION_COLUMN = "position";

  /**
   * This method return a CQL statement to fetch the columns in the primary key with in the table
   * and corresponding keyspace supplied as input. If position>-1 the column is part of the primary
   * key.
   *
   * <p>SELECT column_name, kind, position FROM system_schema.columns WHERE
   * keyspace_name='mykeyspace' AND table_name='mytable' AND position>-1 ALLOW FILTERING;
   */
  protected static Statement primarykeyCQL(String keyspace, String table) {
    return QueryBuilder.select(COLUMN_NAME_COLUMN, KIND_COLUMN, POSITION_COLUMN)
        .from("system_schema", "columns")
        .where(QueryBuilder.eq("keyspace_name", keyspace))
        .and(QueryBuilder.eq("table_name", table))
        .and(QueryBuilder.gt("position", -1))
        .allowFiltering();
  }

  /**
   * This method returns a sorted map containing the columns that make up the (compound) primary key
   * in cassandra.
   *
   * <p>Each key in return value contains a name of a column in the cassandra key. The value for
   * each corresponding key in the map denotes the order in the Cassandra key.
   *
   * <p>Example, given the table:
   *
   * <p>create table mytable ( key_part_one int, key_part_two int, data boolean, PRIMARY
   * KEY(key_part_one, key_part_two) );
   *
   * <p>The method would return the following map:
   *
   * <p>"key_part_one", 0 "key_part_two", 1
   *
   * @param session the session to the Cassandra cluster.
   * @param keyspace they keyspace to query
   * @param table the table to query
   * @return see method description.
   */
  static Map<String, Integer> primaryKeyOrder(Session session, String keyspace, String table) {
    HashMap<String, Integer> returnValue = new HashMap<>();
    Statement select = primarykeyCQL(keyspace, table);
    ResultSet rs = session.execute(select.toString());

    TreeMap<String, String> sortedMap = new TreeMap<>();
    for (Row row : rs) {
      String columnName = row.get(COLUMN_NAME_COLUMN, String.class);
      String kind = row.get(KIND_COLUMN, String.class);
      String position = row.get(POSITION_COLUMN, Integer.class).toString();
      if (kind.equals("clustering")) {
        sortedMap.put("clustering_" + position, columnName);
      } else {
        sortedMap.put(position, columnName);
      }
    }

    List<String> sortedKeyset = new ArrayList<>(sortedMap.keySet());
    for (int i = 0; i < sortedKeyset.size(); i++) {
      returnValue.put(sortedMap.get(sortedKeyset.get(i)), i);
    }

    return returnValue;
  }
}
