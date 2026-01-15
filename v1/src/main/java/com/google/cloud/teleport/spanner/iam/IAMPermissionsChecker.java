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

import com.google.cloud.spanner.DatabaseNotFoundException;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility to check IAM permissions for various GCP resources. */
public class IAMPermissionsChecker {
  private static final Logger LOG = LoggerFactory.getLogger(IAMPermissionsChecker.class);
  private final String projectId;

  private final Spanner spanner;
  private static final String INSTANCE_STRING = "projects/%s/instances/%s";
  private static final String DATABASE_STRING = "projects/%s/instances/%s/database/%s";

  public IAMPermissionsChecker(String projectId) throws GeneralSecurityException, IOException {
    SpannerOptions options = SpannerOptions.newBuilder().setProjectId(projectId).build();

    this.spanner = options.getService();
    this.projectId = projectId;
  }

  @VisibleForTesting
  IAMPermissionsChecker(Spanner spanner, String projectId) {
    this.projectId = projectId;
    this.spanner = spanner;
  }

  /**
   * Checks IAM permissions for a list of requirements. This api should be called once with all the
   * requirements.
   *
   * @return List of results, only missing permissions are included. Empty list indicate all the
   *     requirements are met.
   */
  public IAMCheckResult checkSpannerInstanceRequirements(
      List<String> permissionList, String instanceId) {

    Iterable<String> grantedPermissions =
        spanner.getInstanceAdminClient().getInstance(instanceId).testIAMPermissions(permissionList);

    return new IAMCheckResult(
        String.format(INSTANCE_STRING, projectId, instanceId),
        fetchMissingPermission(permissionList, grantedPermissions));
  }

  /**
   * Checks IAM permissions for a list of requirements. This api should be called once with all the
   * requirements.
   *
   * @return List of results, only missing permissions are included. Empty list indicate all the
   *     requirements are met.
   * @throws DatabaseNotFoundException when database is not found.
   */
  public IAMCheckResult checkSpannerDatabaseRequirements(
      List<String> permissionList, String instanceId, String databaseId)
      throws DatabaseNotFoundException {

    Iterable<String> grantedPermissions =
        spanner
            .getDatabaseAdminClient()
            .getDatabase(instanceId, databaseId)
            .testIAMPermissions(permissionList);
    return new IAMCheckResult(
        String.format(DATABASE_STRING, projectId, instanceId, databaseId),
        fetchMissingPermission(permissionList, grantedPermissions));
  }

  private List<String> fetchMissingPermission(
      List<String> requiredPermission, Iterable<String> grantedPermissions) {

    HashSet<String> grantedPermissionsSet =
        StreamSupport.stream(grantedPermissions.spliterator(), false)
            .collect(Collectors.toCollection(HashSet::new));

    return requiredPermission.stream().filter(p -> !grantedPermissionsSet.contains(p)).toList();
  }
}
