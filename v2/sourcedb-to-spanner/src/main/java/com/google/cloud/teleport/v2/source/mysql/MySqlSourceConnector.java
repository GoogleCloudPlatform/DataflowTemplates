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
package com.google.cloud.teleport.v2.source.mysql;

import com.google.cloud.teleport.v2.source.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.jdbc.JdbcSourceConnector;
import com.google.cloud.teleport.v2.source.jdbc.OptionsToConfigBuilder;
import com.google.cloud.teleport.v2.source.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import java.util.Map.Entry;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** MySQL Source Connector. */
public class MySqlSourceConnector extends JdbcSourceConnector {

  private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceConnector.class);

  @Override
  public JdbcIOWrapperConfig.Builder getJdbcIOWrapperConfigBuilder() {
    return JdbcIOWrapperConfig.builderWithMySqlDefaults();
  }

  @Override
  public SourceSchemaReference getSourceSchemaReference(String dbName, String namespace) {
    JdbcSchemaReference.Builder builder = JdbcSchemaReference.builder();
    // Namespaces are not supported for MySQL
    return SourceSchemaReference.ofJdbc(builder.setDbName(dbName).build());
  }

  @Override
  public void setDataSourceLoginTimeout(BasicDataSource dataSource, long timeoutMs) {
    dataSource.setMaxWaitMillis(timeoutMs);
    String connectivityTimeout = String.valueOf(timeoutMs);
    setConnectionProperty(dataSource, "connectTimeout", connectivityTimeout);
    setConnectionProperty(dataSource, "socketTimeout", connectivityTimeout);
  }

  @Override
  public String getJdbcUrl(
      String host,
      int port,
      String dbName,
      String namespace,
      String connectionProperties,
      Integer fetchSize,
      String sourceDbURL) {
    if (sourceDbURL == null) {
      sourceDbURL = "jdbc:mysql://" + host + ":" + port + "/" + dbName;
      if (StringUtils.isNotBlank(connectionProperties)) {
        sourceDbURL = sourceDbURL + "?" + connectionProperties;
      }
    }
    for (Entry<String, String> entry :
        MySqlConfigDefaults.DEFAULT_MYSQL_URL_PROPERTIES.entrySet()) {
      sourceDbURL =
          OptionsToConfigBuilder.addParamToJdbcUrl(sourceDbURL, entry.getKey(), entry.getValue());
    }
    sourceDbURL = mysqlSetCursorModeIfNeeded(sourceDbURL, fetchSize);
    return sourceDbURL;
  }

  private String mysqlSetCursorModeIfNeeded(String url, Integer fetchSize) {
    if (fetchSize != null && fetchSize == 0) {
      LOG.info(
          "FetchSize is explicitly 0. MySQL cursor mode (useCursorFetch) will not be enabled explicitly.");
      return url;
    }
    LOG.info(
        "FetchSize is {}. Setting MySQL `useCursorFetch=true`.",
        fetchSize == null ? "Auto" : fetchSize);
    return OptionsToConfigBuilder.addParamToJdbcUrl(url, "useCursorFetch", "true");
  }
}
