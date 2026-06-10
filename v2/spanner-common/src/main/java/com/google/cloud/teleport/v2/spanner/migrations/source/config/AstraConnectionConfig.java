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
package com.google.cloud.teleport.v2.spanner.migrations.source.config;

/** Represents the connection configuration for an Astra DB source. */
public class AstraConnectionConfig implements SourceConnectionConfig {

  private String databaseId = "";
  private String astraToken = "";
  private String keySpace = "";
  private String astraDbRegion = "";

  public String getDatabaseId() {
    return databaseId;
  }

  public void setDatabaseId(String databaseId) {
    this.databaseId = databaseId;
  }

  public String getAstraToken() {
    return astraToken;
  }

  public void setAstraToken(String astraToken) {
    this.astraToken = astraToken;
  }

  public String getKeySpace() {
    return keySpace;
  }

  public void setKeySpace(String keySpace) {
    this.keySpace = keySpace;
  }

  public String getAstraDbRegion() {
    return astraDbRegion;
  }

  public void setAstraDbRegion(String astraDbRegion) {
    this.astraDbRegion = astraDbRegion;
  }
}
