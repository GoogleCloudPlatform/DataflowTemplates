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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudresourcemanager.v3.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.v3.model.TestIamPermissionsRequest;
import com.google.api.services.cloudresourcemanager.v3.model.TestIamPermissionsResponse;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.gcp.auth.NullCredentialInitializer;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility to check IAM permissions for various GCP resources. */
public class IAMPermissionsChecker {
  private static final Logger LOG = LoggerFactory.getLogger(IAMPermissionsChecker.class);
  private final Credentials credential;
  private static final String RESOURCE_NAME_FORMAT = "projects/%s";
  private final String projectIdResource;

  private final CloudResourceManager resourceManager;

  public IAMPermissionsChecker(String projectId, GcpOptions gcpOptions)
      throws GeneralSecurityException, IOException {
    this.credential = gcpOptions.getGcpCredential();
    this.projectIdResource = String.format("projects/%s", projectId);
    resourceManager = createCloudResourceManagerService();
  }

  @VisibleForTesting
  IAMPermissionsChecker(
      String projectId, GcpOptions gcpOptions, CloudResourceManager resourceManager) {
    this.credential = gcpOptions.getGcpCredential();
    this.projectIdResource = String.format("projects/%s", projectId);
    this.resourceManager = resourceManager;
  }

  /**
   * Checks IAM permissions for a list of requirements. This api should be called once with all the
   * requirements.
   *
   * @param requirements List of resources and required permissions.
   * @return List of results, only missing permissions are included. Empty list indicate all the
   *     requirements are met.
   */
  public IAMCheckResult check(List<IAMResourceRequirements> requirements) {
    List<String> permissionList =
        requirements.stream()
            .map(IAMResourceRequirements::getPermissions)
            .flatMap(Collection::stream)
            .toList();
    HashSet<String> grantedPermissions =
        new HashSet<>(checkPermission(resourceManager, projectIdResource, permissionList));

    List<String> missingPermissions =
        permissionList.stream()
            .filter(p -> !grantedPermissions.contains(p))
            .collect(Collectors.toList());

    return new IAMCheckResult(projectIdResource, missingPermissions);
  }

  private List<String> checkPermission(
      CloudResourceManager resourceManager, String resourceName, List<String> permissions) {
    try {

      TestIamPermissionsRequest requestBody =
          new TestIamPermissionsRequest().setPermissions(permissions);

      TestIamPermissionsResponse testIamPermissionsResponse =
          resourceManager.projects().testIamPermissions(resourceName, requestBody).execute();

      List<String> granted = testIamPermissionsResponse.getPermissions();
      return granted == null ? Collections.emptyList() : granted;
    } catch (IOException e) {
      LOG.error("Error checking permissions for resource {}", resourceName, e);
      throw new RuntimeException("Failed to check project permissions", e);
    }
  }

  private CloudResourceManager createCloudResourceManagerService()
      throws IOException, GeneralSecurityException {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    HttpRequestInitializer initializer = getHttpRequestInitializer(this.credential);
    CloudResourceManager service =
        new CloudResourceManager.Builder(httpTransport, jsonFactory, initializer)
            .setApplicationName("service-accounts")
            .build();
    return service;
  }

  private static HttpRequestInitializer getHttpRequestInitializer(Credentials credential)
      throws IOException {
    if (credential == null) {
      try {
        return GoogleCredential.getApplicationDefault();
      } catch (Exception e) {
        return new NullCredentialInitializer();
      }
    } else {
      return new HttpCredentialsAdapter(credential);
    }
  }
}
