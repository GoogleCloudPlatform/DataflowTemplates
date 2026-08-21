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
package com.google.cloud.teleport.v2.templates.source;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.api.services.datastream.v1.model.MysqlSourceConfig;
import com.google.api.services.datastream.v1.model.OracleSourceConfig;
import com.google.api.services.datastream.v1.model.PostgresqlSourceConfig;
import com.google.api.services.datastream.v1.model.SourceConfig;
import com.google.cloud.teleport.v2.templates.source.mysql.MySqlDsToSpSourceConnector;
import com.google.cloud.teleport.v2.templates.source.oracle.OracleDsToSpSourceConnector;
import com.google.cloud.teleport.v2.templates.source.postgresql.PostgresqlDsToSpSourceConnector;
import java.util.Set;
import org.junit.Test;

/** Tests for {@link DatastreamToSpannerSourceConnectorRegistry}. */
public class DatastreamToSpannerSourceConnectorRegistryTest {

  @Test
  public void testGetSupportedSourceTypes() {
    Set<String> supportedTypes =
        DatastreamToSpannerSourceConnectorRegistry.getSupportedSourceTypes();
    assertEquals(3, supportedTypes.size());
    assertTrue(supportedTypes.contains("mysql"));
    assertTrue(supportedTypes.contains("postgresql"));
    assertTrue(supportedTypes.contains("oracle"));
  }

  @Test
  public void testGetSourceConnector() {
    assertThat(
        DatastreamToSpannerSourceConnectorRegistry.getSourceConnector("mysql"),
        instanceOf(MySqlDsToSpSourceConnector.class));
    assertThat(
        DatastreamToSpannerSourceConnectorRegistry.getSourceConnector("MYSQL"),
        instanceOf(MySqlDsToSpSourceConnector.class));
    assertThat(
        DatastreamToSpannerSourceConnectorRegistry.getSourceConnector("postgresql"),
        instanceOf(PostgresqlDsToSpSourceConnector.class));
    assertThat(
        DatastreamToSpannerSourceConnectorRegistry.getSourceConnector("oracle"),
        instanceOf(OracleDsToSpSourceConnector.class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSourceConnectorNotFound() {
    DatastreamToSpannerSourceConnectorRegistry.getSourceConnector("invalid");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSourceConnectorEmpty() {
    DatastreamToSpannerSourceConnectorRegistry.getSourceConnector("");
  }

  @Test
  public void testGetSourceTypeFromConfigMysql() {
    SourceConfig config = new SourceConfig().setMysqlSourceConfig(new MysqlSourceConfig());
    assertEquals(
        "mysql", DatastreamToSpannerSourceConnectorRegistry.getSourceTypeFromConfig(config));
  }

  @Test
  public void testGetSourceTypeFromConfigPostgresql() {
    SourceConfig config =
        new SourceConfig().setPostgresqlSourceConfig(new PostgresqlSourceConfig());
    assertEquals(
        "postgresql", DatastreamToSpannerSourceConnectorRegistry.getSourceTypeFromConfig(config));
  }

  @Test
  public void testGetSourceTypeFromConfigOracle() {
    SourceConfig config = new SourceConfig().setOracleSourceConfig(new OracleSourceConfig());
    assertEquals(
        "oracle", DatastreamToSpannerSourceConnectorRegistry.getSourceTypeFromConfig(config));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSourceTypeFromConfigUnsupported() {
    SourceConfig config = new SourceConfig();
    DatastreamToSpannerSourceConnectorRegistry.getSourceTypeFromConfig(config);
  }
}
