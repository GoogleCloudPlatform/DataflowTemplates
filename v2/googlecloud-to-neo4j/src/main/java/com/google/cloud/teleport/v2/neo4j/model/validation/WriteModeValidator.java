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
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.RelationshipTarget;
import org.neo4j.importer.v1.validation.SpecificationValidationResult.Builder;
import org.neo4j.importer.v1.validation.SpecificationValidator;

public class WriteModeValidator implements SpecificationValidator {

  private static final String ERROR_CODE = "DFWM-001";
  private final Set<String> paths;

  public WriteModeValidator() {
    paths = new LinkedHashSet<>();
  }

  @Override
  public void visitNodeTarget(int index, NodeTarget target) {
    if (target.getWriteMode() == null) {
      paths.add(String.format("$.targets.nodes[%d].write_mode", index));
    }
  }

  @Override
  public void visitRelationshipTarget(int index, RelationshipTarget target) {
    if (target.getWriteMode() == null) {
      paths.add(String.format("$.targets.relationships[%d].write_mode", index));
    }
  }

  @Override
  public boolean report(Builder builder) {
    paths.forEach(
        path ->
            builder.addError(
                path,
                ERROR_CODE,
                String.format("%s is missing: please specify a write mode", path)));
    return paths.isEmpty();
  }
}
