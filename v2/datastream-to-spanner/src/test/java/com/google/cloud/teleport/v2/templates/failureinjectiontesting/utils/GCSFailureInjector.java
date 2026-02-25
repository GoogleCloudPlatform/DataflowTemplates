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
package com.google.cloud.teleport.v2.templates.failureinjectiontesting.utils;

import com.google.cloud.Identity;
import com.google.cloud.Policy;
import com.google.cloud.Role;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCSFailureInjector {

  private static final Logger LOG = LoggerFactory.getLogger(GCSFailureInjector.class);

  public static void removeGCSPermissions(String bucket, String serviceAccountEmail) {
    Storage storage = StorageOptions.getDefaultInstance().getService();
    Policy originalPolicy = storage.getIamPolicy(bucket);

    String role = "organizations/433637338589/roles/GcsBucketOwner";
    Policy modifiedPolicy =
        originalPolicy.toBuilder()
            .removeIdentity(
                Role.of("organizations/433637338589/roles/GcsBucketOwner"),
                Identity.serviceAccount(serviceAccountEmail))
            .build();

    storage.setIamPolicy(bucket, modifiedPolicy);

    LOG.info("Successfully removed service account from the role " + role);
  }

  public static void addGCSPermissions(String bucket, String serviceAccountEmail) {
    Storage storage = StorageOptions.getDefaultInstance().getService();
    Policy originalPolicy = storage.getIamPolicy(bucket);

    String role = "organizations/433637338589/roles/GcsBucketOwner";

    Policy modifiedPolicy =
        originalPolicy.toBuilder()
            .addIdentity(Role.of(role), Identity.serviceAccount(serviceAccountEmail))
            .build();

    storage.setIamPolicy(bucket, modifiedPolicy);

    LOG.info("Successfully added service account to the role " + role);
  }
}
