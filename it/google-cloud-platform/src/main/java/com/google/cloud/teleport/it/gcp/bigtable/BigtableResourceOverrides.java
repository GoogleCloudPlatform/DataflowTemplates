/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.it.gcp.bigtable;

import org.apache.commons.lang3.Validate;

/**
 * Bigtable Admin APIs might not allow creating resources with certain configurations yet. In order
 * to test against resources created manually specifying BigtableResourceOverrides allows to keep
 * test code intact as if new instances and tables are created, but no resource creation will take
 * place
 */
public class BigtableResourceOverrides {

  private final String instanceId;

  /**
   * InstanceId to be used for testing. createInstance() will skip creating a new one and just use
   * provided instanceId for further testing. createTable() will skip creating a new table as if it
   * successfully created one. createAppProfile() will skip creating a new appProfile as if it was
   * successfully created one.
   *
   * @param instanceIdOverride Instance ID to use.
   */
  public BigtableResourceOverrides(String instanceIdOverride) {
    Validate.notBlank(instanceIdOverride, "InstanceID override is empty");
    this.instanceId = instanceIdOverride;
  }

  public String getInstanceId() {
    return instanceId;
  }
}
