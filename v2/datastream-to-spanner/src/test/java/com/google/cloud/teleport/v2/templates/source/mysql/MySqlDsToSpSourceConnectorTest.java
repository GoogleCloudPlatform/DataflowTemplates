/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.source.mysql;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.datastream.v1.model.MysqlSourceConfig;
import com.google.api.services.datastream.v1.model.OracleSourceConfig;
import com.google.api.services.datastream.v1.model.SourceConfig;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.source.SourceConstants;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContext;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventConvertorTest;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

/** Unit tests for {@link MySqlDsToSpSourceConnector}. */
public final class MySqlDsToSpSourceConnectorTest {

  private final MySqlDsToSpSourceConnector connector = new MySqlDsToSpSourceConnector();

  private JsonNode getJsonNode(String json) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    return mapper.readTree(json);
  }

  @Test
  public void testGetSourceType() {
    assertEquals(SourceConstants.MYSQL_SOURCE_TYPE, connector.getSourceType());
  }

  @Test
  public void testMatches() {
    SourceConfig mysqlConfig = new SourceConfig().setMysqlSourceConfig(new MysqlSourceConfig());
    SourceConfig oracleConfig = new SourceConfig().setOracleSourceConfig(new OracleSourceConfig());

    assertTrue(connector.matchesSourceConfig(mysqlConfig));
    assertFalse(connector.matchesSourceConfig(oracleConfig));
  }

  @Test
  public void testGetSortOrder() {
    assertEquals(
        MySqlDsToSpSourceConnector.MYSQL_SORT_ORDER,
        connector.getSortOrder(Dialect.GOOGLE_STANDARD_SQL));
    assertEquals(
        MySqlDsToSpSourceConnector.MYSQL_SORT_ORDER_PG_DIALECT,
        connector.getSortOrder(Dialect.POSTGRESQL));
  }

  @Test
  public void testCreateChangeEventContext() throws Exception {
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();
    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users2");
    changeEvent.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, 1615159728L);
    changeEvent.put(MySqlDsToSpSourceConnector.MYSQL_LOGFILE_KEY, "file1.log");
    changeEvent.put(MySqlDsToSpSourceConnector.MYSQL_LOGPOSITION_KEY, 1L);
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, SourceConstants.MYSQL_SOURCE_TYPE);

    ChangeEventContext context =
        connector.createChangeEventContext(
            getJsonNode(changeEvent.toString()), ddl, ddl, "shadow_");

    assertThat(context, instanceOf(MySqlChangeEventContext.class));
  }
}
