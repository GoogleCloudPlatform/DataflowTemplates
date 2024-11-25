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

import java.util.LinkedHashSet;
import java.util.Set;
import org.neo4j.importer.v1.targets.RelationshipTarget;
import org.neo4j.importer.v1.validation.SpecificationValidationResult.Builder;
import org.neo4j.importer.v1.validation.SpecificationValidator;

public class NodeMatchModeValidator implements SpecificationValidator {

  private static final String ERROR_CODE = "RNMM-001";

  private final Set<String> paths;

  public NodeMatchModeValidator() {
    paths = new LinkedHashSet<>();
  }

  @Override
  public void visitRelationshipTarget(int index, RelationshipTarget target) {
    if (target.getNodeMatchMode() == null) {
      paths.add(String.format("$.targets.relationships[%d].node_match_mode", index));
    }
  }

  @Override
  public boolean report(Builder builder) {
    paths.forEach(
        path ->
            builder.addError(
                path,
                ERROR_CODE,
                String.format("%s is missing: please specify a node match mode", path)));
    return paths.isEmpty();
  }
}
