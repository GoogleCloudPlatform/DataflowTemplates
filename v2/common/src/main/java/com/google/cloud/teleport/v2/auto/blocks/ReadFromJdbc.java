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
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadFromJdbc implements TemplateTransform<ReadFromJdbcOptions> {

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(ReadFromJdbc.class);

  @Outputs(String.class)
  public PCollection<String> read(Pipeline pipeline, ReadFromJdbcOptions options) {
    return pipeline.apply(
        "readFromJdbc",
        JdbcIO.<String>read()
            .withDataSourceConfiguration(buildDataSourceConfig(options))
            .withQuery(options.getQuery())
            .withCoder(StringUtf8Coder.of())
            .withRowMapper(new ResultSetToJSONString()));
  }

  /**
   * {@link JdbcIO.RowMapper} implementation to convert Jdbc ResultSet rows to UTF-8 encoded JSONs.
   */
  public static class ResultSetToJSONString implements JdbcIO.RowMapper<String> {

    @Override
    public String mapRow(ResultSet resultSet) throws Exception {
      ResultSetMetaData metaData = resultSet.getMetaData();
      JSONObject json = new JSONObject();

      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        Object value = resultSet.getObject(i);

        // JSONObject.put() does not support null values. The exception is JSONObject.NULL
        if (value == null) {
          json.put(metaData.getColumnLabel(i), JSONObject.NULL);
          continue;
        }

        switch (metaData.getColumnTypeName(i).toLowerCase()) {
          case "clob":
            Clob clobObject = resultSet.getClob(i);
            if (clobObject.length() > Integer.MAX_VALUE) {
              LOG.warn(
                  "The Clob value size {} in column {} exceeds 2GB and will be truncated.",
                  clobObject.length(),
                  metaData.getColumnLabel(i));
            }
            json.put(
                metaData.getColumnLabel(i), clobObject.getSubString(1, (int) clobObject.length()));
            break;
          default:
            json.put(metaData.getColumnLabel(i), value);
        }
      }
      return json.toString();
    }
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
