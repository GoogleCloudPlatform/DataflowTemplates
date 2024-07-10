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
package com.google.cloud.teleport.v2.spanner.utils;

import java.util.Map;

public class MigrationTransformationResponse {
  private Map<String, Object> responseRow;
  private boolean isEventFiltered;

  public Map<String, Object> getResponseRow() {
    return responseRow;
  }

  public void setResponseRow(Map<String, Object> responseRow) {
    this.responseRow = responseRow;
  }

  public boolean isEventFiltered() {
    return isEventFiltered;
  }

  public void setEventFiltered(boolean eventFiltered) {
    isEventFiltered = eventFiltered;
  }

  public MigrationTransformationResponse(Map<String, Object> responseRow, boolean isEventFiltered) {
    this.responseRow = responseRow;
    this.isEventFiltered = isEventFiltered;
  }

  @Override
  public String toString() {
    return "MigrationTransformationResponse{"
        + "responseRow="
        + responseRow
        + ", isEventFiltered="
        + isEventFiltered
        + '}';
  }
}
