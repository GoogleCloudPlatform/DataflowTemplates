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

/**
 * Represents the IAM permissions required on a specific GCP resource. This can be expanded to
 * contain resource name when permission specific to resources need to be validated.
 */
public class IAMResourceRequirements {
  private final List<String> permissions;

  public IAMResourceRequirements(List<String> permissions) {
    if (permissions == null || permissions.isEmpty()) {
      throw new IllegalArgumentException("Permissions list must not be empty");
    }
    this.permissions = new ArrayList<>(permissions);
  }

  public List<String> getPermissions() {
    return new ArrayList<>(permissions);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + "permissions=" + permissions + '}';
  }
}
