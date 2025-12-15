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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.api.services.cloudresourcemanager.v3.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.v3.model.TestIamPermissionsResponse;
import com.google.auth.Credentials;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for IAMPermissionsChecker. */
@RunWith(JUnit4.class)
public class IAMPermissionsCheckerTest {

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private CloudResourceManager mockResourceManager;

  @Mock private GcpOptions mockGcpOptions;
  @Mock private Credentials mockCredentials;

  private IAMPermissionsChecker checker;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
    when(mockGcpOptions.getGcpCredential()).thenReturn(mockCredentials);
    checker = new IAMPermissionsChecker("test-project", mockGcpOptions, mockResourceManager);
  }

  @Test
  public void testCheck_allPermissionsGranted() throws IOException {
    // Arrange
    List<String> requiredPermissions = Arrays.asList("p1", "p2");
    IAMResourceRequirements requirements = new IAMResourceRequirements(requiredPermissions);
    TestIamPermissionsResponse response =
        new TestIamPermissionsResponse().setPermissions(requiredPermissions);
    when(mockResourceManager.projects().testIamPermissions(anyString(), any()).execute())
        .thenReturn(response);

    // Act
    IAMCheckResult result = checker.check(Collections.singletonList(requirements));

    // Assert
    assertTrue(result.isPermissionsAvailable());
    assertTrue(result.getMissingPermissions().isEmpty());
  }

  @Test
  public void testCheck_somePermissionsMissing() throws IOException {
    // Arrange
    List<String> requiredPermissions = Arrays.asList("p1", "p2", "p3");
    List<String> grantedPermissions = Arrays.asList("p1", "p3");
    IAMResourceRequirements requirements = new IAMResourceRequirements(requiredPermissions);
    TestIamPermissionsResponse response =
        new TestIamPermissionsResponse().setPermissions(grantedPermissions);
    when(mockResourceManager.projects().testIamPermissions(anyString(), any()).execute())
        .thenReturn(response);

    // Act
    IAMCheckResult result = checker.check(Collections.singletonList(requirements));

    // Assert
    assertEquals(Arrays.asList("p2"), result.getMissingPermissions());
  }

  @Test
  public void testCheck_noPermissionsGranted() throws IOException {
    // Arrange
    List<String> requiredPermissions = Arrays.asList("p1", "p2");
    IAMResourceRequirements requirements = new IAMResourceRequirements(requiredPermissions);
    TestIamPermissionsResponse response = new TestIamPermissionsResponse(); // No permissions
    when(mockResourceManager.projects().testIamPermissions(anyString(), any()).execute())
        .thenReturn(response);

    // Act
    IAMCheckResult result = checker.check(Collections.singletonList(requirements));

    // Assert
    assertEquals(requiredPermissions, result.getMissingPermissions());
  }
}
