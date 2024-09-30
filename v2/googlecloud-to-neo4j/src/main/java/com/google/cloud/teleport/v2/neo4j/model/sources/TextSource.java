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
import org.neo4j.importer.v1.sources.Source;

public abstract class TextSource implements Source {

  private final String name;

  private final List<String> header;

  public TextSource(String name, List<String> header) {
    this.name = name;
    this.header = header;
  }

  public List<String> getHeader() {
    return header;
  }

  @Override
  public String getType() {
    return "text";
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TextSource)) {
      return false;
    }
    TextSource that = (TextSource) o;
    return Objects.equals(name, that.name) && Objects.equals(header, that.header);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, header);
  }

  @Override
  public String toString() {
    return "TextSource{" + "name='" + name + '\'' + ", header=" + header + '}';
  }
}
