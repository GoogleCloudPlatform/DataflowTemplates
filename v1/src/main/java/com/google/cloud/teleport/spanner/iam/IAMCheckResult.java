/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.spanner.iam;

import java.util.ArrayList;
import java.util.List;

/** Represents the result of an IAM permission check on a specific resource. */
public class IAMCheckResult {
  private final String resourceName;
  private final List<String> missingPermissions;

  public IAMCheckResult(String resourceName, List<String> missingPermissions) {
    this.resourceName = resourceName;
    this.missingPermissions = new ArrayList<>(missingPermissions);
  }

  public String getResourceName() {
    return resourceName;
  }

  public List<String> getMissingPermissions() {
    return new ArrayList<>(missingPermissions);
  }

  public boolean isPermissionsAvailable() {
    return missingPermissions.isEmpty();
  }

  @Override
  public String toString() {
    return "IAMCheckResult{"
        + "resourceName='"
        + resourceName
        + '\''
        + ", missingPermissions="
        + missingPermissions
        + ", success="
        + isPermissionsAvailable()
        + '}';
  }
}
