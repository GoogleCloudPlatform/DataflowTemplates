/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.transformer;

import static com.google.cloud.teleport.v2.constants.Constants.EVENT_TABLE_NAME_KEY;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

/** Convert {@link ResultSet} object to a {@link JsonNode}. */
public class ResultSetToJsonNode implements JdbcIO.RowMapper<JsonNode> {
  public static JdbcIO.RowMapper<JsonNode> create(String table) {
    return new ResultSetToJsonNode(table);
  }

  private String table;

  private ResultSetToJsonNode(String table) {
    this.table = table;
  }

  @Override
  public JsonNode mapRow(ResultSet resultSet) throws Exception {
    return convertToChangeEventJson(resultSet, table);
  }

  public JsonNode convertToChangeEventJson(ResultSet rs, String tableName) throws SQLException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    ObjectNode node = mapper.createObjectNode();
    ResultSetMetaData metaData = rs.getMetaData();
    node.put(EVENT_TABLE_NAME_KEY, tableName);
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      JsonNode valueNode = mapper.convertValue(rs.getObject(i), JsonNode.class);
      node.set(metaData.getColumnName(i), valueNode);
    }
    return node;
  }
}
