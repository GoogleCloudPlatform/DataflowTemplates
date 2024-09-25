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
package com.google.cloud.teleport.v2.neo4j.model.validation;

import com.google.cloud.teleport.v2.neo4j.model.sources.TextSource;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.neo4j.importer.v1.sources.Source;
import org.neo4j.importer.v1.validation.SpecificationValidationResult.Builder;
import org.neo4j.importer.v1.validation.SpecificationValidator;

public class DuplicateTextHeaderValidator implements SpecificationValidator {

  private static final String ERROR_CODE = "DFTH-001";

  private final Map<String, Duplicate<String>> sourcePathToDuplicates;

  public DuplicateTextHeaderValidator() {
    sourcePathToDuplicates = new LinkedHashMap<>();
  }

  @Override
  public void visitSource(int index, Source source) {
    if (!(source instanceof TextSource)) {
      return;
    }
    TextSource textSource = (TextSource) source;
    List<String> header = textSource.getHeader();
    if (header == null) {
      return;
    }
    String sourcePath = String.format("$.sources[%d].header", index);
    findDuplicates(header).forEach(duplicate -> sourcePathToDuplicates.put(sourcePath, duplicate));
  }

  @Override
  public boolean report(Builder builder) {
    if (sourcePathToDuplicates.isEmpty()) {
      return false;
    }
    sourcePathToDuplicates.forEach(
        (sourcePath, duplicate) -> {
          builder.addError(
              sourcePath,
              ERROR_CODE,
              String.format(
                  "%s defines column \"%s\" %d times, it must be defined at most once",
                  sourcePath, duplicate.getValue(), duplicate.getCount()));
        });
    return true;
  }

  private static List<Duplicate<String>> findDuplicates(List<String> header) {
    Map<String, Integer> counts = new LinkedHashMap<>();
    header.forEach(column -> counts.merge(column, 1, Integer::sum));
    return counts.entrySet().stream()
        .filter(entry -> entry.getValue() > 1)
        .map(entry -> new Duplicate<>(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  private static class Duplicate<T> {
    private final T value;
    private final int count;

    public Duplicate(T value, int count) {
      this.value = value;
      this.count = count;
    }

    public T getValue() {
      return value;
    }

    public int getCount() {
      return count;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Duplicate)) {
        return false;
      }
      Duplicate<?> duplicate = (Duplicate<?>) o;
      return count == duplicate.count && Objects.equals(value, duplicate.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value, count);
    }
  }
}
