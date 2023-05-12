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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

/**
 * Convenience object for invoking SQL query as well as providing descriptions for read and cast
 * phase of transform.
 */
public class SqlQuerySpec {

  private final String readDescription;
  private final String castDescription;
  private final String sql;

  public SqlQuerySpec(String readDescription, String castDescription, String sql) {
    this.readDescription = readDescription;
    this.castDescription = castDescription;
    this.sql = sql;
  }

  public String getReadDescription() {
    return readDescription;
  }

  public String getCastDescription() {
    return castDescription;
  }

  public String getSql() {
    return sql;
  }

  public static class SqlQuerySpecBuilder {

    private String readDescription;
    private String castDescription;
    private String sql;

    public SqlQuerySpecBuilder readDescription(String readDescription) {
      this.readDescription = readDescription;
      return this;
    }

    public SqlQuerySpecBuilder castDescription(String castDescription) {
      this.castDescription = castDescription;
      return this;
    }

    public SqlQuerySpecBuilder sql(String sql) {
      this.sql = sql;
      return this;
    }

    public SqlQuerySpec build() {
      return new SqlQuerySpec(readDescription, castDescription, sql);
    }
  }
}
