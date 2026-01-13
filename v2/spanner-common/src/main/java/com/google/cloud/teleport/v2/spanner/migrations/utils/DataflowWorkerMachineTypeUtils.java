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
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class DataflowWorkerMachineTypeUtils {

  private static final Map<String, Double> STANDARD_MACHINE_TYPE_MEMORY_GB = new HashMap<>();

  static {
    // -------------------------------------------------------------------------
    // N1 Series (General Purpose - Legacy/Default)
    // Source:
    // https://cloud.google.com/compute/docs/general-purpose-machines#n1_machines
    // -------------------------------------------------------------------------

    // Standard (3.75 GB per vCPU)
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-standard-1", 3.75);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-standard-2", 7.50);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-standard-4", 15.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-standard-8", 30.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-standard-16", 60.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-standard-32", 120.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-standard-64", 240.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-standard-96", 360.00);

    // High-Memory (6.50 GB per vCPU)
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-highmem-2", 13.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-highmem-4", 26.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-highmem-8", 52.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-highmem-16", 104.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-highmem-32", 208.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-highmem-64", 416.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-highmem-96", 624.00);

    // High-CPU (0.90 GB per vCPU)
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-highcpu-2", 1.80);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-highcpu-4", 3.60);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-highcpu-8", 7.20);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-highcpu-16", 14.40);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-highcpu-32", 28.80);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-highcpu-64", 57.60);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n1-highcpu-96", 86.40);

    // -------------------------------------------------------------------------
    // N2 Series (General Purpose - Modern)
    // Source:
    // https://cloud.google.com/compute/docs/general-purpose-machines#n2_machines
    // -------------------------------------------------------------------------

    // Standard (4.00 GB per vCPU)
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-standard-2", 8.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-standard-4", 16.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-standard-8", 32.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-standard-16", 64.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-standard-32", 128.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-standard-48", 192.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-standard-64", 256.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-standard-80", 320.00);

    // High-Memory (8.00 GB per vCPU)
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-highmem-2", 16.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-highmem-4", 32.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-highmem-8", 64.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-highmem-16", 128.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-highmem-32", 256.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-highmem-48", 384.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-highmem-64", 512.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-highmem-80", 640.00);

    // High-CPU (1.00 GB per vCPU)
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-highcpu-2", 2.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-highcpu-4", 4.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-highcpu-8", 8.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-highcpu-16", 16.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("n2-highcpu-32", 32.00);

    // -------------------------------------------------------------------------
    // E2 Series (Cost-Optimized / Dynamic)
    // Source:
    // https://cloud.google.com/compute/docs/general-purpose-machines#e2_machines
    // -------------------------------------------------------------------------

    // Standard (4.00 GB per vCPU)
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("e2-standard-2", 8.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("e2-standard-4", 16.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("e2-standard-8", 32.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("e2-standard-16", 64.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("e2-standard-32", 128.00);

    // High-Memory (8.00 GB per vCPU)
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("e2-highmem-2", 16.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("e2-highmem-4", 32.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("e2-highmem-8", 64.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("e2-highmem-16", 128.00);

    // -------------------------------------------------------------------------
    // C2 Series (Compute-Optimized)
    // Source:
    // https://cloud.google.com/compute/docs/compute-optimized-machines#c2_machines
    // -------------------------------------------------------------------------

    // Standard (4.00 GB per vCPU)
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("c2-standard-4", 16.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("c2-standard-8", 32.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("c2-standard-16", 64.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("c2-standard-30", 120.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("c2-standard-60", 240.00);

    // -------------------------------------------------------------------------
    // M1 Series (Memory-Optimized)
    // Source: https://cloud.google.com/compute/docs/memory-optimized-machines
    // -------------------------------------------------------------------------

    // Ultra-High Memory
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("m1-ultramem-40", 961.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("m1-ultramem-80", 1922.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("m1-ultramem-160", 3844.00);
    STANDARD_MACHINE_TYPE_MEMORY_GB.put("m1-megamem-96", 1433.60);
  }

  public static Double getWorkerMemoryGB(String workerMachineType) {
    Preconditions.checkArgument(
        workerMachineType != null && !StringUtils.isBlank(workerMachineType),
        "workerMachineType cannot be null or empty.");

    // Handle standard machine types
    Double memoryGb = STANDARD_MACHINE_TYPE_MEMORY_GB.get(workerMachineType);
    if (memoryGb != null) {
      return memoryGb;
    }
    return null;
  }

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
