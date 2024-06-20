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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.neo4j.importer.v1.sources.Source;
import org.neo4j.importer.v1.targets.EntityTarget;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.RelationshipTarget;
import org.neo4j.importer.v1.validation.SpecificationValidationResult.Builder;
import org.neo4j.importer.v1.validation.SpecificationValidator;

public class DuplicateAggregateFieldNameValidator implements SpecificationValidator {

  private static final String ERROR_CODE = "DFAF-001";

  private final Map<String, Set<String>> sourceFields;
  private final Map<String, String> duplicatedAggregateFields;

  public DuplicateAggregateFieldNameValidator() {
    this.sourceFields = new HashMap<>();
    this.duplicatedAggregateFields = new LinkedHashMap<>();
  }

  @Override
  public void visitSource(int index, Source source) {
    if (source instanceof TextSource) {
      sourceFields.put(source.getName(), new HashSet<>(((TextSource) source).getHeader()));
    }
  }

  @Override
  public void visitNodeTarget(int index, NodeTarget target) {
    visitEntity(index, target);
  }

  @Override
  public void visitRelationshipTarget(int index, RelationshipTarget target) {
    visitEntity(index, target);
  }

  @Override
  public boolean report(Builder builder) {
    duplicatedAggregateFields.forEach(
        (path, field) ->
            builder.addError(
                path,
                ERROR_CODE,
                String.format(
                    "%s \"%s\" is already defined in the target's source header", path, field)));
    return !duplicatedAggregateFields.isEmpty();
  }

  private void visitEntity(int index, EntityTarget target) {
    var source = target.getSource();
    if (!sourceFields.containsKey(source)) {
      return;
    }
    var sourceFields = this.sourceFields.get(source);
    var sourceTransformations = target.getSourceTransformations();
    if (sourceTransformations == null) {
      return;
    }
    var aggregations = sourceTransformations.getAggregations();
    var group = target instanceof NodeTarget ? "nodes" : "relationships";
    for (int i = 0; i < aggregations.size(); i++) {
      var path =
          String.format(
              "$.targets.%s[%d].source_transformations.aggregations[%d].field_name",
              group, index, i);
      var aggregatedField = aggregations.get(i).getFieldName();
      if (sourceFields.contains(aggregatedField)) {
        duplicatedAggregateFields.put(path, aggregatedField);
      }
    }
  }
}
