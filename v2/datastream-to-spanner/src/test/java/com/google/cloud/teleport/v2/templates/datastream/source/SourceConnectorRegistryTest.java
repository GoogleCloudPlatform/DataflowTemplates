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
package com.google.cloud.teleport.v2.templates.datastream.source;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.api.services.datastream.v1.model.MysqlSourceConfig;
import com.google.api.services.datastream.v1.model.OracleSourceConfig;
import com.google.api.services.datastream.v1.model.PostgresqlSourceConfig;
import com.google.api.services.datastream.v1.model.SourceConfig;
import com.google.cloud.teleport.v2.templates.datastream.source.mysql.MySqlSourceConnector;
import com.google.cloud.teleport.v2.templates.datastream.source.oracle.OracleSourceConnector;
import com.google.cloud.teleport.v2.templates.datastream.source.postgresql.PostgresqlSourceConnector;
import java.util.Set;
import org.junit.Test;

/** Tests for {@link SourceConnectorRegistry}. */
public class SourceConnectorRegistryTest {

  @Test
  public void testGetSupportedSourceTypes() {
    Set<String> supportedTypes = SourceConnectorRegistry.getSupportedSourceTypes();
    assertEquals(3, supportedTypes.size());
    assertTrue(supportedTypes.contains("mysql"));
    assertTrue(supportedTypes.contains("postgresql"));
    assertTrue(supportedTypes.contains("oracle"));
  }

  @Test
  public void testGetSourceConnector() {
    assertThat(
        SourceConnectorRegistry.getSourceConnector("mysql"),
        instanceOf(MySqlSourceConnector.class));
    assertThat(
        SourceConnectorRegistry.getSourceConnector("MYSQL"),
        instanceOf(MySqlSourceConnector.class));
    assertThat(
        SourceConnectorRegistry.getSourceConnector("postgresql"),
        instanceOf(PostgresqlSourceConnector.class));
    assertThat(
        SourceConnectorRegistry.getSourceConnector("oracle"),
        instanceOf(OracleSourceConnector.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSourceConnectorNotFound() {
    SourceConnectorRegistry.getSourceConnector("invalid");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSourceConnectorEmpty() {
    SourceConnectorRegistry.getSourceConnector("");
  }

  @Test
  public void testGetSourceTypeFromConfigMysql() {
    SourceConfig config = new SourceConfig().setMysqlSourceConfig(new MysqlSourceConfig());
    assertEquals("mysql", SourceConnectorRegistry.getSourceTypeFromConfig(config));
  }

  @Test
  public void testGetSourceTypeFromConfigPostgresql() {
    SourceConfig config =
        new SourceConfig().setPostgresqlSourceConfig(new PostgresqlSourceConfig());
    assertEquals("postgresql", SourceConnectorRegistry.getSourceTypeFromConfig(config));
  }

  @Test
  public void testGetSourceTypeFromConfigOracle() {
    SourceConfig config = new SourceConfig().setOracleSourceConfig(new OracleSourceConfig());
    assertEquals("oracle", SourceConnectorRegistry.getSourceTypeFromConfig(config));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSourceTypeFromConfigUnsupported() {
    SourceConfig config = new SourceConfig();
    SourceConnectorRegistry.getSourceTypeFromConfig(config);
  }
}
