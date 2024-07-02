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
package com.google.cloud.teleport.v2.neo4j.model.sources;

import java.util.Objects;
import org.neo4j.importer.v1.sources.Source;

public class BigQuerySource implements Source {

  private final String name;
  private final String query;

  public BigQuerySource(String name, String query) {

    this.name = name;
    this.query = query;
  }

  @Override
  public String getType() {
    return "bigquery";
  }

  @Override
  public String getName() {
    return name;
  }

  public String getQuery() {
    return query;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BigQuerySource)) {
      return false;
    }
    BigQuerySource that = (BigQuerySource) o;
    return Objects.equals(name, that.name) && Objects.equals(query, that.query);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, query);
  }

  @Override
  public String toString() {
    return "BigQuerySource{" + "name='" + name + '\'' + ", query='" + query + '\'' + '}';
  }
}
