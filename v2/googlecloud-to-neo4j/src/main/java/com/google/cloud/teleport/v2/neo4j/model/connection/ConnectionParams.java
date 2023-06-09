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

import com.google.cloud.teleport.v2.neo4j.utils.FileSystemUtils;
import java.io.Serializable;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper object for connection params. */
public class ConnectionParams implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionParams.class);

  public String serverUrl, database, authType, username, password;

  public ConnectionParams(String neoConnectionUri) {

    String neoConnectionJsonStr = "{}";
    try {
      neoConnectionJsonStr = FileSystemUtils.getPathContents(neoConnectionUri);
    } catch (Exception e) {
      LOG.error("Unable to read {} neo4j configuration: ", neoConnectionUri, e);
    }

    try {
      JSONObject neoConnectionJson = new JSONObject(neoConnectionJsonStr);
      serverUrl = neoConnectionJson.getString("server_url");
      if (neoConnectionJson.has("auth_type")) {
        authType = neoConnectionJson.getString("auth_type");
      } else {
        authType = "basic";
      }
      database =
          neoConnectionJson.has("database") ? neoConnectionJson.getString("database") : "neo4j";
      username = neoConnectionJson.getString("username");
      password = neoConnectionJson.getString("pwd");

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
