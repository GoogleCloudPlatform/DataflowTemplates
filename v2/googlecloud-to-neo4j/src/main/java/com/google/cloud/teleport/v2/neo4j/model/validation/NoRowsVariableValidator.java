/*
 * Copyright (C) 2026 Google LLC
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

import java.util.Set;
import org.neo4j.importer.v1.validation.interpolation.InterpolationValidationResult;
import org.neo4j.importer.v1.validation.interpolation.InterpolationVariableValidator;
import org.neo4j.importer.v1.validation.interpolation.plugin.NotNullVariableNameValidator;

public class NoRowsVariableValidator implements InterpolationVariableValidator {

  @Override
  public Set<Class<? extends InterpolationVariableValidator>> requires() {
    return Set.of(NotNullVariableNameValidator.class);
  }

  @Override
  public InterpolationValidationResult validate(String name, Object value) {
    if (name.equals("rows")) {
      return InterpolationValidationResult.builder()
          .addError(
              "Interpolation variables cannot be named 'rows', as it is reserved for query targets")
          .build();
    }
    return InterpolationValidationResult.success();
  }
}
