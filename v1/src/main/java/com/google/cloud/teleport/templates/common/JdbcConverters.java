/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.templates.common;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.options.CommonTemplateOptions;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common code for Teleport JdbcToBigQuery. */
public class JdbcConverters {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcConverters.class);

  /** Interface used by the JdbcToBigQuery pipeline to accept user input. */
  public interface JdbcToBigQueryOptions extends CommonTemplateOptions {

    @TemplateParameter.Text(
        order = 1,
        regexes = {"^.+$"},
        description = "Cloud Storage paths for JDBC drivers",
        helpText = "Comma separate Cloud Storage paths for JDBC drivers.",
        example = "gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar")
    ValueProvider<String> getDriverJars();

    void setDriverJars(ValueProvider<String> driverJar);

    @TemplateParameter.Text(
        order = 2,
        regexes = {"^.+$"},
        description = "JDBC driver class name.",
        helpText = "JDBC driver class name to use.",
        example = "com.mysql.jdbc.Driver")
    ValueProvider<String> getDriverClassName();

    void setDriverClassName(ValueProvider<String> driverClassName);

    @TemplateParameter.Text(
        order = 3,
        regexes = {
          "(^jdbc:[a-zA-Z0-9/:@.?_+!*=&-;]+$)|(^([A-Za-z0-9+/]{4}){1,}([A-Za-z0-9+/]{0,3})={0,3})"
        },
        description =
            "JDBC connection URL string. Connection string can be passed in as plaintext or as "
                + "a base64 encoded string encrypted by Google Cloud KMS.",
        helpText = "Url connection string to connect to the JDBC source.",
        example = "jdbc:mysql://some-host:3306/sampledb")
    ValueProvider<String> getConnectionURL();

    void setConnectionURL(ValueProvider<String> connectionURL);

    @TemplateParameter.Text(
        order = 4,
        optional = true,
        regexes = {"^[a-zA-Z0-9_;!*&=@#-:\\/]+$"},
        description = "JDBC connection property string.",
        helpText =
            "Properties string to use for the JDBC connection. Format of the string must be [propertyName=property;]*.",
        example = "unicode=true;characterEncoding=UTF-8")
    @Validation.Required
    ValueProvider<String> getConnectionProperties();

    void setConnectionProperties(ValueProvider<String> connectionProperties);

    @TemplateParameter.Text(
        order = 5,
        optional = true,
        regexes = {"^.+$"},
        description = "JDBC connection username.",
        helpText =
            "User name to be used for the JDBC connection. User name can be passed in as plaintext "
                + "or as a base64 encoded string encrypted by Google Cloud KMS.")
    ValueProvider<String> getUsername();

    void setUsername(ValueProvider<String> username);

    @TemplateParameter.Password(
        order = 6,
        optional = true,
        description = "JDBC connection password.",
        helpText =
            "Password to be used for the JDBC connection. Password can be passed in as plaintext "
                + "or as a base64 encoded string encrypted by Google Cloud KMS.")
    ValueProvider<String> getPassword();

    void setPassword(ValueProvider<String> password);

    @TemplateParameter.Text(
        order = 7,
        regexes = {"^.+$"},
        description = "JDBC source SQL query.",
        helpText = "Query to be executed on the source to extract the data.",
        example = "select * from sampledb.sample_table")
    ValueProvider<String> getQuery();

    void setQuery(ValueProvider<String> query);

    @TemplateParameter.BigQueryTable(
        order = 8,
        description = "BigQuery output table",
        helpText =
            "BigQuery table location to write the output to. The table's schema must match the "
                + "input objects.")
    ValueProvider<String> getOutputTable();

    void setOutputTable(ValueProvider<String> value);

    @TemplateParameter.GcsWriteFolder(
        order = 9,
        description = "Temporary directory for BigQuery loading process",
        helpText = "Temporary directory for BigQuery loading process",
        example = "gs://your-bucket/your-files/temp_dir")
    ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);

    @TemplateParameter.KmsEncryptionKey(
        order = 10,
        optional = true,
        description = "Google Cloud KMS key",
        helpText =
            "If this parameter is provided, password, user name and connection string should all be passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt",
        example =
            "projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key")
    ValueProvider<String> getKMSEncryptionKey();

    void setKMSEncryptionKey(ValueProvider<String> keyName);

    @TemplateParameter.Boolean(
        order = 11,
        optional = true,
        description = "Whether to use column alias to map the rows.",
        helpText =
            "If enabled (set to true) the pipeline will consider column alias (\"AS\") instead of the column name to map the rows to BigQuery. Defaults to false.")
    @Default.Boolean(false)
    ValueProvider<Boolean> getUseColumnAlias();

    void setUseColumnAlias(ValueProvider<Boolean> useColumnAlias);
  }

  /** Factory method for {@link ResultSetToTableRow}. */
  public static JdbcIO.RowMapper<TableRow> getResultSetToTableRow(
      ValueProvider<Boolean> useColumnAlias) {
    return new ResultSetToTableRow(useColumnAlias);
  }

  /**
   * {@link JdbcIO.RowMapper} implementation to convert Jdbc ResultSet rows to UTF-8 encoded JSONs.
   */
  private static class ResultSetToTableRow implements JdbcIO.RowMapper<TableRow> {

    static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
    static DateTimeFormatter datetimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss.SSSSSS");
    static SimpleDateFormat timestampFormatter =
        new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSSSSXXX");

    private ValueProvider<Boolean> useColumnAlias;

    public ResultSetToTableRow(ValueProvider<Boolean> useColumnAlias) {
      this.useColumnAlias = useColumnAlias;
    }

    @Override
    public TableRow mapRow(ResultSet resultSet) throws Exception {

      ResultSetMetaData metaData = resultSet.getMetaData();

      TableRow outputTableRow = new TableRow();

      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        if (resultSet.getObject(i) == null) {
          outputTableRow.set(getColumnRef(metaData, i), resultSet.getObject(i));
          continue;
        }

        /*
         * DATE:      EPOCH MILLISECONDS -> yyyy-MM-dd
         * DATETIME:  EPOCH MILLISECONDS -> yyyy-MM-dd hh:mm:ss.SSSSSS
         * TIMESTAMP: EPOCH MILLISECONDS -> yyyy-MM-dd hh:mm:ss.SSSSSSXXX
         *
         * MySQL drivers have ColumnTypeName in all caps and postgres in small case
         */
        switch (metaData.getColumnTypeName(i).toLowerCase()) {
          case "date":
            outputTableRow.set(
                getColumnRef(metaData, i), dateFormatter.format(resultSet.getDate(i)));
            break;
          case "datetime":
            outputTableRow.set(
                getColumnRef(metaData, i),
                datetimeFormatter.format((TemporalAccessor) resultSet.getObject(i)));
            break;
          case "timestamp":
            outputTableRow.set(
                getColumnRef(metaData, i), timestampFormatter.format(resultSet.getTimestamp(i)));
            break;
          case "clob":
            Clob clobObject = resultSet.getClob(i);
            if (clobObject.length() > Integer.MAX_VALUE) {
              LOG.warn(
                  "The Clob value size {} in column {} exceeds 2GB and will be truncated.",
                  clobObject.length(),
                  getColumnRef(metaData, i));
            }
            outputTableRow.set(
                getColumnRef(metaData, i), clobObject.getSubString(1, (int) clobObject.length()));
            break;
          default:
            outputTableRow.set(getColumnRef(metaData, i), resultSet.getObject(i));
        }
      }

      return outputTableRow;
    }

    protected String getColumnRef(ResultSetMetaData metaData, int index) throws SQLException {
      if (useColumnAlias != null && useColumnAlias.get() != null && useColumnAlias.get()) {
        String columnLabel = metaData.getColumnLabel(index);
        if (columnLabel != null && !columnLabel.isEmpty()) {
          return columnLabel;
        }
      }

      return metaData.getColumnName(index);
    }
  }
}
