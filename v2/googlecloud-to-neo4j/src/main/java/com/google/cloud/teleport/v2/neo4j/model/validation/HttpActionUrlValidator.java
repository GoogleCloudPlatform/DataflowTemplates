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

import com.google.cloud.teleport.v2.neo4j.actions.HttpAction;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.importer.v1.actions.Action;
import org.neo4j.importer.v1.validation.SpecificationValidationResult.Builder;
import org.neo4j.importer.v1.validation.SpecificationValidator;

public class HttpActionUrlValidator implements SpecificationValidator {

  private static final String ERROR_CODE = "DFHT-002";

  private final List<Integer> offendingActionIndices = new ArrayList<>();

  @Override
  public void visitAction(int index, Action action) {
    if (!(action instanceof HttpAction httpAction)) {
      return;
    }
    if (httpAction.url().isBlank()) {
      offendingActionIndices.add(index);
    }
  }

  @Override
  public boolean report(Builder builder) {
    offendingActionIndices.forEach(
        index -> {
          String path = "$.actions[%d].sql".formatted(index);
          builder.addError(path, ERROR_CODE, String.format("%s must not be blank", path));
        });
    return !offendingActionIndices.isEmpty();
  }
}
