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
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseNotFoundException;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.Spanner;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for IAMPermissionsChecker. */
@RunWith(JUnit4.class)
public class IAMPermissionsCheckerTest {

  private static final String PROJECT_ID = "test-project";
  private static final String INSTANCE_ID = "test-instance";
  private static final String DATABASE_ID = "test-database";

  @Mock private Spanner spanner;
  @Mock private InstanceAdminClient instanceAdminClient;
  @Mock private DatabaseAdminClient databaseAdminClient;
  @Mock private Instance instance;
  @Mock private Database database;

  private IAMPermissionsChecker checker;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    checker = new IAMPermissionsChecker(spanner, PROJECT_ID);

    when(spanner.getInstanceAdminClient()).thenReturn(instanceAdminClient);
    when(spanner.getDatabaseAdminClient()).thenReturn(databaseAdminClient);
    when(instanceAdminClient.getInstance(INSTANCE_ID)).thenReturn(instance);
    when(databaseAdminClient.getDatabase(INSTANCE_ID, DATABASE_ID)).thenReturn(database);
  }

  @Test
  public void testCheckSpannerInstanceRequirements_allPermissionsGranted() {
    List<String> requiredPermissions =
        Arrays.asList("spanner.instances.get", "spanner.instances.update");
    when(instance.testIAMPermissions(requiredPermissions)).thenReturn(requiredPermissions);

    IAMCheckResult result =
        checker.checkSpannerInstanceRequirements(requiredPermissions, INSTANCE_ID);

    assertTrue(result.getMissingPermissions().isEmpty());
    assertEquals(
        String.format("projects/%s/instances/%s", PROJECT_ID, INSTANCE_ID),
        result.getResourceName());
  }

  @Test
  public void testCheckSpannerInstanceRequirements_somePermissionsMissing() {
    List<String> requiredPermissions =
        Arrays.asList("spanner.instances.get", "spanner.instances.update");
    List<String> grantedPermissions = Collections.singletonList("spanner.instances.get");
    when(instance.testIAMPermissions(requiredPermissions)).thenReturn(grantedPermissions);

    IAMCheckResult result =
        checker.checkSpannerInstanceRequirements(requiredPermissions, INSTANCE_ID);

    assertEquals(
        Collections.singletonList("spanner.instances.update"), result.getMissingPermissions());
    assertEquals(
        String.format("projects/%s/instances/%s", PROJECT_ID, INSTANCE_ID),
        result.getResourceName());
  }

  @Test
  public void testCheckSpannerDatabaseRequirements_allPermissionsGranted() {
    List<String> requiredPermissions =
        Arrays.asList("spanner.databases.get", "spanner.databases.update");
    when(database.testIAMPermissions(requiredPermissions)).thenReturn(requiredPermissions);

    IAMCheckResult result =
        checker.checkSpannerDatabaseRequirements(requiredPermissions, INSTANCE_ID, DATABASE_ID);

    assertTrue(result.getMissingPermissions().isEmpty());
    assertEquals(
        String.format("projects/%s/instances/%s/database/%s", PROJECT_ID, INSTANCE_ID, DATABASE_ID),
        result.getResourceName());
  }

  @Test
  public void testCheckSpannerDatabaseRequirements_somePermissionsMissing() {
    List<String> requiredPermissions =
        Arrays.asList("spanner.databases.get", "spanner.databases.update");
    List<String> grantedPermissions = Collections.singletonList("spanner.databases.get");
    when(database.testIAMPermissions(requiredPermissions)).thenReturn(grantedPermissions);

    IAMCheckResult result =
        checker.checkSpannerDatabaseRequirements(requiredPermissions, INSTANCE_ID, DATABASE_ID);

    assertEquals(
        Collections.singletonList("spanner.databases.update"), result.getMissingPermissions());
    assertEquals(
        String.format("projects/%s/instances/%s/database/%s", PROJECT_ID, INSTANCE_ID, DATABASE_ID),
        result.getResourceName());
  }

  @Test(expected = DatabaseNotFoundException.class)
  public void testCheckSpannerDatabaseRequirements_databaseNotFound() {
    when(databaseAdminClient.getDatabase(INSTANCE_ID, DATABASE_ID))
        .thenThrow(DatabaseNotFoundException.class);
    checker.checkSpannerDatabaseRequirements(
        Collections.singletonList("spanner.databases.get"), INSTANCE_ID, DATABASE_ID);
  }
}
