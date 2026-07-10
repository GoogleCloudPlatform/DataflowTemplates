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
package com.google.cloud.teleport.v2.source;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.source.cassandra.CassandraSrcToSpSourceConnector;
import com.google.cloud.teleport.v2.source.jdbc.AbstractJdbcSrcToSpSourceConnector;
import com.google.cloud.teleport.v2.source.mysql.MySqlSrcToSpSourceConnector;
import com.google.cloud.teleport.v2.source.postgres.PostgresSrcToSpSourceConnector;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link SourceConnectorFactory}. */
@RunWith(JUnit4.class)
public class SourceConnectorFactoryTest {

  @Test
  public void testConstructor() {
    assertThat(new SourceConnectorFactory()).isNotNull();
  }

  @Test
  public void testGetSourceConnectorByDialect_optionsCassandra() {
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setSourceDbDialect(SourceDbToSpannerOptions.CASSANDRA_SOURCE_DIALECT);

    ISrcToSpSourceConnector connector = SourceConnectorFactory.getSourceConnectorByDialect(options);

    assertThat(connector).isInstanceOf(CassandraSrcToSpSourceConnector.class);
  }

  @Test
  public void testGetSourceConnectorByDialect_optionsAstraDb() {
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setSourceDbDialect(SourceDbToSpannerOptions.ASTRA_DB_SOURCE_DIALECT);

    ISrcToSpSourceConnector connector = SourceConnectorFactory.getSourceConnectorByDialect(options);

    assertThat(connector).isInstanceOf(CassandraSrcToSpSourceConnector.class);
  }

  @Test
  public void testGetSourceConnectorByDialect_optionsMySql() {
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setSourceDbDialect(SourceDbToSpannerOptions.MYSQL_SOURCE_DIALECT);

    ISrcToSpSourceConnector connector = SourceConnectorFactory.getSourceConnectorByDialect(options);

    assertThat(connector).isInstanceOf(MySqlSrcToSpSourceConnector.class);
  }

  @Test
  public void testGetSourceConnectorByDialect_optionsPostgres() {
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setSourceDbDialect(SourceDbToSpannerOptions.PG_SOURCE_DIALECT);

    ISrcToSpSourceConnector connector = SourceConnectorFactory.getSourceConnectorByDialect(options);

    assertThat(connector).isInstanceOf(PostgresSrcToSpSourceConnector.class);
  }

  @Test
  public void testGetSourceConnectorByDialect_optionsUnsupportedDialect() {
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setSourceDbDialect("unsupported_db");

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> SourceConnectorFactory.getSourceConnectorByDialect(options));

    assertThat(thrown)
        .hasMessageThat()
        .contains("Unsupported source database dialect: unsupported_db");
  }

  @Test
  public void testGetSourceConnectorByDialect_optionsNullDialect() {
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setSourceDbDialect(null);

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> SourceConnectorFactory.getSourceConnectorByDialect(options));

    assertThat(thrown).hasMessageThat().contains("Unsupported source database dialect: null");
  }

  @Test
  public void testGetSourceConnectorByDialect_sqlDialectMySql() {
    AbstractJdbcSrcToSpSourceConnector connector =
        SourceConnectorFactory.getSourceJdbcConnectorByDialect(SQLDialect.MYSQL);

    assertThat(connector).isInstanceOf(MySqlSrcToSpSourceConnector.class);
  }

  @Test
  public void testGetSourceConnectorByDialect_sqlDialectPostgres() {
    AbstractJdbcSrcToSpSourceConnector connector =
        SourceConnectorFactory.getSourceJdbcConnectorByDialect(SQLDialect.POSTGRESQL);

    assertThat(connector).isInstanceOf(PostgresSrcToSpSourceConnector.class);
  }

  @Test
  public void testGetSourceConnectorByDialect_sqlDialectNull() {
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> SourceConnectorFactory.getSourceJdbcConnectorByDialect((SQLDialect) null));

    assertThat(thrown).hasMessageThat().contains("Unsupported SQL dialect: null");
  }

  @Test
  public void testGetSourceConnectorBySourceType_mysql() {
    ISrcToSpSourceConnector connector =
        SourceConnectorFactory.getSourceConnectorBySourceType(Constants.MYSQL_SOURCE_TYPE);
    assertThat(connector).isInstanceOf(MySqlSrcToSpSourceConnector.class);
  }

  @Test
  public void testGetSourceConnectorBySourceType_postgres() {
    ISrcToSpSourceConnector connector =
        SourceConnectorFactory.getSourceConnectorBySourceType(Constants.POSTGRES_SOURCE_TYPE);
    assertThat(connector).isInstanceOf(PostgresSrcToSpSourceConnector.class);
  }

  @Test
  public void testGetSourceConnectorBySourceType_cassandra() {
    ISrcToSpSourceConnector connector =
        SourceConnectorFactory.getSourceConnectorBySourceType(Constants.CASSANDRA_SOURCE_TYPE);
    assertThat(connector).isInstanceOf(CassandraSrcToSpSourceConnector.class);
  }

  @Test
  public void testGetSourceConnectorBySourceType_unsupported() {
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> SourceConnectorFactory.getSourceConnectorBySourceType("unsupported"));
  }

  @Test
  public void testGetSourceConnectorBySourceType_null() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SourceConnectorFactory.getSourceConnectorBySourceType(null));
  }
}
