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

public class ExternalTextSource extends TextSource {
  private final List<String> urls;
  private final TextFormat format;
  private final String columnDelimiter;
  private final String lineSeparator;

  public ExternalTextSource(
      String name,
      List<String> urls,
      List<String> header,
      TextFormat format,
      String columnDelimiter,
      String lineSeparator) {
    super(name, header);
    this.urls = urls;
    this.format = format;
    this.columnDelimiter = columnDelimiter;
    this.lineSeparator = lineSeparator;
  }

  public List<String> getUrls() {
    return urls;
  }

  public TextFormat getFormat() {
    return format;
  }

  public String getColumnDelimiter() {
    return columnDelimiter;
  }

  public String getLineSeparator() {
    return lineSeparator;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExternalTextSource)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ExternalTextSource that = (ExternalTextSource) o;
    return Objects.equals(urls, that.urls)
        && format == that.format
        && Objects.equals(columnDelimiter, that.columnDelimiter)
        && Objects.equals(lineSeparator, that.lineSeparator);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), urls, format, columnDelimiter, lineSeparator);
  }
}
