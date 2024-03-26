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
package com.google.cloud.teleport.v2.reader;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Convert {@link ResultSet} object to a {@link JsonNode}.
 */
public class ResultSetToJsonNode implements JdbcIO.RowMapper<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(ResultSetToJsonNode.class);

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
        node.put(Constants.EVENT_TABLE_NAME_KEY, tableName);
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            JsonNode valueNode;
            switch (metaData.getColumnType(i)) {
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    String timestamp = rs.getObject(i).toString();
                    // Replace spaces with T if any. JDBC returns 2 different formats for java.sql.Types.Timestamp.
                    // For mysql type Datetime, format is 1998-01-23T12:45:56 while for MySQL timestamp, format is 2022-08-05 08:23:11.0.
                    timestamp = timestamp.replaceAll("\\s", "T");
                    valueNode = mapper.convertValue(timestamp, JsonNode.class);
                    break;
                case Types.DATE:// TODO: for year source type, strip off dd and mm.
                case Types.TIME:
                    valueNode = mapper.convertValue(rs.getObject(i).toString(), JsonNode.class);
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    byte[] bytes = rs.getBytes(i);
                    valueNode = mapper.convertValue(Hex.encodeHexString(bytes), JsonNode.class);
                    break;
                default:
                    // TODO: Handle MySQL BIT correctly. BIT and other booleans have the same java.sql.Type but the former should be read as bytes while the latter should be read as bools.
                    valueNode = mapper.convertValue(rs.getObject(i), JsonNode.class);
                    break;
            }
            node.set(metaData.getColumnName(i), valueNode);
        }
        return node;
    }
}