/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.auto.blocks;

import static com.google.cloud.teleport.v2.utils.KMSUtils.maybeDecrypt;

import com.google.cloud.teleport.metadata.auto.Outputs;
import com.google.cloud.teleport.metadata.auto.TemplateTransform;
import com.google.cloud.teleport.v2.auto.options.ReadFromJdbcOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.SchemaUtil;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class ReadFromJdbc implements TemplateTransform<ReadFromJdbcOptions> {

  @Outputs(Row.class)
  public PCollection<Row> read(Pipeline pipeline, ReadFromJdbcOptions options) {
    JdbcIO.DataSourceConfiguration dataSourceConfig = buildDataSourceConfig(options);
    Schema schema =
        JdbcIO.ReadRows.inferBeamSchema(
            JdbcIO.DataSourceProviderFromDataSourceConfiguration.of(dataSourceConfig).apply(null),
            options.getQuery());

    if (options.getPartitionColumn() != null && options.getTable() != null) {
      // Read with Partitions
      // TODO(pranavbhandari): Support readWithPartitions for other data types.
      JdbcIO.ReadWithPartitions<Row, Long> readIO =
          JdbcIO.<Row>readWithPartitions()
              .withDataSourceConfiguration(dataSourceConfig)
              .withTable(options.getTable())
              .withPartitionColumn(options.getPartitionColumn())
              .withRowMapper(SchemaUtil.BeamRowMapper.of(schema));
      if (options.getNumPartitions() != null) {
        readIO = readIO.withNumPartitions(options.getNumPartitions());
      }
      if (options.getLowerBound() != null && options.getUpperBound() != null) {
        readIO =
            readIO.withLowerBound(options.getLowerBound()).withUpperBound(options.getUpperBound());
      }
      return pipeline.apply("Read from JDBC with Partitions", readIO);
    }

    return pipeline.apply(
        "readFromJdbc",
        JdbcIO.<Row>read()
            .withDataSourceConfiguration(dataSourceConfig)
            .withQuery(options.getQuery())
            .withCoder(RowCoder.of(schema))
            .withRowMapper(SchemaUtil.BeamRowMapper.of(schema)));
  }

  static JdbcIO.DataSourceConfiguration buildDataSourceConfig(ReadFromJdbcOptions options) {
    JdbcIO.DataSourceConfiguration dataSourceConfiguration =
        JdbcIO.DataSourceConfiguration.create(
            StaticValueProvider.of(options.getDriverClassName()),
            maybeDecrypt(options.getConnectionUrl(), options.getKMSEncryptionKey()));
    if (options.getDriverJars() != null) {
      dataSourceConfiguration = dataSourceConfiguration.withDriverJars(options.getDriverJars());
    }
    if (options.getUsername() != null) {
      dataSourceConfiguration =
          dataSourceConfiguration.withUsername(
              maybeDecrypt(options.getUsername(), options.getKMSEncryptionKey()));
    }
    if (options.getPassword() != null) {
      dataSourceConfiguration =
          dataSourceConfiguration.withPassword(
              maybeDecrypt(options.getPassword(), options.getKMSEncryptionKey()));
    }
    if (options.getConnectionProperties() != null) {
      dataSourceConfiguration =
          dataSourceConfiguration.withConnectionProperties(options.getConnectionProperties());
    }
    return dataSourceConfiguration;
  }

  @Override
  public Class<ReadFromJdbcOptions> getOptionsClass() {
    return ReadFromJdbcOptions.class;
  }
}
