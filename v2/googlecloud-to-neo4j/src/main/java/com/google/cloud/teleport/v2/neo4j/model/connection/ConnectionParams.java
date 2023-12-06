/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.model.connection;

import com.google.cloud.teleport.v2.neo4j.options.Neo4jFlexTemplateOptions;
import com.google.cloud.teleport.v2.neo4j.utils.FileSystemUtils;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import java.io.Serializable;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper object for connection params. */
public class ConnectionParams implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionParams.class);

  public String serverUrl, database, authType, username, password;

  public ConnectionParams(Neo4jFlexTemplateOptions options) {
    String json;
    String secretId = options.getNeo4jConnectionSecretId();
    if (StringUtils.isNotBlank(secretId)) {
      json = SecretManagerUtils.getSecret(secretId);
    } else {
      json = readGcsResource(options.getNeo4jConnectionUri());
    }
    initialize(json);
  }

  private void initialize(String rawJson) {
    try {
      JSONObject json = new JSONObject(rawJson);
      serverUrl = json.getString("server_url");
      authType = json.has("auth_type") ? json.getString("auth_type") : "basic";
      database = json.has("database") ? json.getString("database") : "neo4j";
      username = json.getString("username");
      password = json.getString("pwd");

    } catch (Exception e) {
      LOG.error("Unable to parse Neo4j JSON connection metadata: ", e);
      throw new IllegalStateException(e);
    }
  }

  private static String readGcsResource(String uri) {
    try {
      return FileSystemUtils.getPathContents(uri);
    } catch (Exception e) {
      LOG.error("Unable to read {} Neo4j configuration: ", uri, e);
      throw new IllegalStateException(e);
    }
  }
}
