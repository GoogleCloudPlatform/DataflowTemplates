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

import java.util.List;
import java.util.Objects;

public class InlineTextSource extends TextSource {

  private final List<List<Object>> data;

  public InlineTextSource(String name, List<List<Object>> data, List<String> header) {
    super(name, header);
    this.data = data;
  }

  public List<List<Object>> getData() {
    return data;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InlineTextSource)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    InlineTextSource that = (InlineTextSource) o;
    return Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), data);
  }

  @Override
  public String toString() {
    return "InlineTextSource{" + "data=" + data + "} " + super.toString();
  }
}
