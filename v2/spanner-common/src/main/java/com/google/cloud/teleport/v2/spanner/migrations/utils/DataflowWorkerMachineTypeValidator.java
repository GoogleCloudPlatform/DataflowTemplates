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
package com.google.cloud.teleport.v2.spanner.migrations.utils;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

public class DataflowWorkerMachineTypeValidator {

  public static void validateMachineSpecs(String workerMachineType, Integer minCPUs) {
    Preconditions.checkArgument(
        workerMachineType != null && !StringUtils.isBlank(workerMachineType),
        "Policy Violation: You must specify a workerMachineType with at least %s vCPUs.",
        minCPUs);

    // Handle custom machine types first, format is custom-{vCPU}-{RAM}
    if (workerMachineType.startsWith("custom-")) {
      String[] parts = workerMachineType.split("-");
      Preconditions.checkArgument(
          parts.length == 3,
          "Invalid custom machine type format: '%s'. Expected format: custom-{vCPU}-{RAM}.",
          workerMachineType);
      Integer vCpus = null;
      try {
        vCpus = Integer.parseInt(parts[1]);
      } catch (NumberFormatException e) {
        Preconditions.checkArgument(
            false, "Invalid vCPU number in custom machine type: '%s'", workerMachineType);
      }
      Preconditions.checkArgument(
          vCpus >= minCPUs,
          "Policy Violation: Custom machine type '%s' has %s vCPUs. Minimum allowed is %s. Please use a higher machine type.",
          workerMachineType,
          vCpus,
          minCPUs);
    } else {
      // Handle standard machine types.
      java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(".*-(\\d+)$");
      java.util.regex.Matcher matcher = pattern.matcher(workerMachineType);

      if (matcher.find()) {
        Integer vCpus = null;
        try {
          vCpus = Integer.parseInt(matcher.group(1));
        } catch (NumberFormatException e) {
          Preconditions.checkArgument(
              false, "Invalid vCPU number in machine type: '%s'", workerMachineType);
        }
        Preconditions.checkArgument(
            vCpus >= minCPUs,
            "Policy Violation: Machine type '%s' has %s vCPUs. Minimum allowed is %s.",
            workerMachineType,
            vCpus,
            minCPUs);
      } else {
        Preconditions.checkArgument(
            false,
            "Unknown machine type format: '%s'. Please use a standard machine type (e.g., n1-standard-4) or a custom machine type (e.g., custom-4-4096) with at least %s vCPUs.",
            workerMachineType,
            minCPUs);
      }
    }
  }
}
