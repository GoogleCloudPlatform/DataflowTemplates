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

import com.google.cloud.teleport.v2.neo4j.model.sources.BigQuerySource;
import java.util.LinkedHashSet;
import java.util.Set;
import org.neo4j.importer.v1.sources.Source;
import org.neo4j.importer.v1.validation.SpecificationValidationResult;
import org.neo4j.importer.v1.validation.SpecificationValidator;

public class BigQuerySourceProjectDatasetValidator implements SpecificationValidator {

  private static final String ERROR_CODE = "DFBQ-001";
  private final Set<String> paths = new LinkedHashSet<>();

  @Override
  public void visitSource(int index, Source source) {
    if (!(source instanceof BigQuerySource)) {
      return;
    }

    var queryTempProject = ((BigQuerySource) source).getQueryTempProject();
    var queryTempDataset = ((BigQuerySource) source).getQueryTempDataset();

    if (queryTempProject != null && queryTempDataset == null) {
      paths.add(String.format("$.sources[%d]", index));
    }
  }

  @Override
  public boolean report(SpecificationValidationResult.Builder builder) {
    paths.forEach(
        path ->
            builder.addError(
                path,
                ERROR_CODE,
                String.format(
                    "%s query_temp_project is provided, but query_temp_dataset is missing", path)));
    return paths.isEmpty();
  }
}
