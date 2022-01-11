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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

/** Common code for Teleport JdbcToBigQuery. */
public class JdbcConverters {

  /** Interface used by the JdbcToBigQuery pipeline to accept user input. */
  public interface JdbcToBigQueryOptions extends PipelineOptions {

    @Description(
        "Comma separate list of driver class/dependency jar file GCS paths "
            + "for example "
            + "gs://<some-bucket>/driver_jar1.jar,gs://<some_bucket>/driver_jar2.jar")
    ValueProvider<String> getDriverJars();

    void setDriverJars(ValueProvider<String> driverJar);

    @Description("The JDBC driver class name. " + "for example: com.mysql.jdbc.Driver")
    ValueProvider<String> getDriverClassName();

    void setDriverClassName(ValueProvider<String> driverClassName);

    @Description(
        "The JDBC connection URL string. " + "for example: jdbc:mysql://some-host:3306/sampledb")
    ValueProvider<String> getConnectionURL();

    void setConnectionURL(ValueProvider<String> connectionURL);

    @Description(
        "JDBC connection property string. " + "for example: unicode=true&characterEncoding=UTF-8")
    ValueProvider<String> getConnectionProperties();

    void setConnectionProperties(ValueProvider<String> connectionProperties);

    @Description("JDBC connection user name. ")
    ValueProvider<String> getUsername();

    void setUsername(ValueProvider<String> username);

    @Description("JDBC connection password. ")
    ValueProvider<String> getPassword();

    void setPassword(ValueProvider<String> password);

    @Description("Source data query string. " + "for example: select * from sampledb.sample_table")
    ValueProvider<String> getQuery();

    void setQuery(ValueProvider<String> query);

    @Description(
        "BigQuery Table spec to write the output to"
            + "for example: some-project-id:somedataset.sometable")
    ValueProvider<String> getOutputTable();

    void setOutputTable(ValueProvider<String> value);

    @Description("Temporary directory for BigQuery loading process")
    ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);

    @Description(
        "KMS Encryption Key should be in the format"
            + " projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}")
    ValueProvider<String> getKMSEncryptionKey();

    void setKMSEncryptionKey(ValueProvider<String> keyName);

    @Description(
        "Comma seperated algorithms to disable. If this value is set to \"none\" then"
            + " jdk.tls.disabledAlgorithms is set to \"\". Use with care, as the algorithms"
            + " disabled by default are known to have either vulnerabilities or performance issues."
            + " for example: SSLv3, RC4.")
    ValueProvider<String> getDisabledAlgorithms();

    void setDisabledAlgorithms(ValueProvider<String> disabledAlgorithms);
  }

  /** Factory method for {@link ResultSetToTableRow}. */
  public static JdbcIO.RowMapper<TableRow> getResultSetToTableRow() {
    return new ResultSetToTableRow();
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

    @Override
    public TableRow mapRow(ResultSet resultSet) throws Exception {

      ResultSetMetaData metaData = resultSet.getMetaData();

      TableRow outputTableRow = new TableRow();

      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        if (resultSet.getObject(i) == null) {
          outputTableRow.set(metaData.getColumnName(i), resultSet.getObject(i));
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
                metaData.getColumnName(i), dateFormatter.format(resultSet.getObject(i)));
            break;
          case "datetime":
            outputTableRow.set(
                metaData.getColumnName(i),
                datetimeFormatter.format((TemporalAccessor) resultSet.getObject(i)));
            break;
          case "timestamp":
            outputTableRow.set(
                metaData.getColumnName(i), timestampFormatter.format(resultSet.getObject(i)));
            break;
          default:
            outputTableRow.set(metaData.getColumnName(i), resultSet.getObject(i));
        }
      }

      return outputTableRow;
    }
  }
}
