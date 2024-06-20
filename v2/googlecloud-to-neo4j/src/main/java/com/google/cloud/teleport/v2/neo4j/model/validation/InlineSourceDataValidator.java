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

import com.google.cloud.teleport.v2.neo4j.model.sources.InlineTextSource;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.neo4j.importer.v1.sources.Source;
import org.neo4j.importer.v1.validation.SpecificationValidationResult;
import org.neo4j.importer.v1.validation.SpecificationValidator;

public class InlineSourceDataValidator implements SpecificationValidator {

  private static final String ERROR_CODE = "DFID-001";

  private final Map<String, CountMismatch> pathToCountMismatch;

  public InlineSourceDataValidator() {
    pathToCountMismatch = new LinkedHashMap<>();
  }

  @Override
  public void visitSource(int sourceIndex, Source source) {
    if (!(source instanceof InlineTextSource)) {
      return;
    }
    InlineTextSource inlineSource = (InlineTextSource) source;
    int columnCount = inlineSource.getHeader().size();
    List<List<Object>> data = inlineSource.getData();
    String path = "$.sources[%d].data[%d]";
    for (int rowIndex = 0; rowIndex < data.size(); rowIndex++) {
      int rowColumnCount = data.get(rowIndex).size();
      if (rowColumnCount < columnCount) {
        String rowPath = String.format(path, sourceIndex, rowIndex);
        pathToCountMismatch.put(rowPath, new CountMismatch(columnCount, rowColumnCount));
      }
    }
  }

  @Override
  public boolean report(SpecificationValidationResult.Builder builder) {
    if (pathToCountMismatch.isEmpty()) {
      return false;
    }
    pathToCountMismatch.forEach(
        (path, count) -> {
          builder.addError(
              path,
              ERROR_CODE,
              String.format(
                  "row defines %d column(s), expected at least %d",
                  count.getActualCount(), count.getExpectedCount()));
        });
    return true;
  }

  private static class CountMismatch {
    private final int expectedCount;
    private final int actualCount;

    public CountMismatch(int expectedCount, int actualCount) {
      this.expectedCount = expectedCount;
      this.actualCount = actualCount;
    }

    public int getExpectedCount() {
      return expectedCount;
    }

    public int getActualCount() {
      return actualCount;
    }

    @Override
    public boolean equals(Object object) {
      if (this == object) {
        return true;
      }
      if (object == null || getClass() != object.getClass()) {
        return false;
      }
      CountMismatch that = (CountMismatch) object;
      return expectedCount == that.expectedCount && actualCount == that.actualCount;
    }

    @Override
    public int hashCode() {
      return Objects.hash(expectedCount, actualCount);
    }
  }
}
