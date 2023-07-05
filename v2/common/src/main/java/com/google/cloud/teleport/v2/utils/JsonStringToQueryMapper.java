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
package com.google.cloud.teleport.v2.utils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The {@link JdbcIO.PreparedStatementSetter} implementation for mapping JSON string to query. */
public class JsonStringToQueryMapper implements JdbcIO.PreparedStatementSetter<String> {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(JsonStringToQueryMapper.class);

  List<String> keyOrder;

  public JsonStringToQueryMapper(List<String> keyOrder) {
    this.keyOrder = keyOrder;
  }

  public void setParameters(String element, PreparedStatement query) throws SQLException {
    try {
      JSONObject object = new JSONObject(element);
      for (int i = 0; i < keyOrder.size(); i++) {
        String key = keyOrder.get(i);
        if (!object.has(key) || object.get(key) == JSONObject.NULL) {
          query.setNull(i + 1, Types.NULL);
        } else {
          query.setObject(i + 1, object.get(key));
        }
      }
    } catch (Exception e) {
      LOG.error("Error while mapping Pub/Sub strings to JDBC", e);
    }
  }
}
