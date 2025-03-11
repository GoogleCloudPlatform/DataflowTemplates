/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.clickhouse.utils;

// utils class for ClickHouse
public class ClickHouseUtils {

  public static String setJDBCCredentials(String jdbcURL, String username, String password) {
    String credentials =
        password != null
            ? String.format("user=%s&password=%s", username, password)
            : String.format("user=%s", username);
    return jdbcURL + (jdbcURL.contains("?") ? "&" : "?") + credentials;
  }
}
