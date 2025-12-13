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

import com.google.common.collect.ImmutableList;
import java.util.List;

public class IAMRequirementsCreator {
  private static final List<String> SPANNER_PERMISSIONS =
      ImmutableList.of(
          "spanner.databases.beginOrRollbackReadWriteTransaction",
          "spanner.databases.beginPartitionedDmlTransaction",
          "spanner.databases.beginReadOnlyTransaction",
          "spanner.databases.create",
          "spanner.databases.drop",
          "spanner.databases.get",
          "spanner.databases.getDdl",
          "spanner.databases.list",
          "spanner.databases.partitionQuery",
          "spanner.databases.partitionRead",
          "spanner.databases.read",
          "spanner.databases.select",
          "spanner.databases.update",
          "spanner.databases.updateDdl",
          "spanner.databases.write",
          "spanner.instances.get",
          "spanner.instances.list");

  public static IAMResourceRequirements createSpannerResourceRequirement() {
    return new IAMResourceRequirements(SPANNER_PERMISSIONS);
  }
}
