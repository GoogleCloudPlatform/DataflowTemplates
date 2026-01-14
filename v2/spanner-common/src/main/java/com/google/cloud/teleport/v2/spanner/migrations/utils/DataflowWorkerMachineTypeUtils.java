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

  private static final Map<String, MachineSpec> STANDARD_MACHINE_TYPE_SPECS = new HashMap<>();

  static {
    // -------------------------------------------------------------------------
    // N1 Series (General Purpose - Legacy/Default)
    // Source:
    // https://cloud.google.com/compute/docs/general-purpose-machines#n1_machines
    // -------------------------------------------------------------------------

    // Standard (3.75 GB per vCPU)
    STANDARD_MACHINE_TYPE_SPECS.put("n1-standard-1", new MachineSpec(3.75, 1));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-standard-2", new MachineSpec(7.50, 2));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-standard-4", new MachineSpec(15.00, 4));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-standard-8", new MachineSpec(30.00, 8));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-standard-16", new MachineSpec(60.00, 16));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-standard-32", new MachineSpec(120.00, 32));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-standard-64", new MachineSpec(240.00, 64));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-standard-96", new MachineSpec(360.00, 96));

    // High-Memory (6.50 GB per vCPU)
    STANDARD_MACHINE_TYPE_SPECS.put("n1-highmem-2", new MachineSpec(13.00, 2));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-highmem-4", new MachineSpec(26.00, 4));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-highmem-8", new MachineSpec(52.00, 8));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-highmem-16", new MachineSpec(104.00, 16));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-highmem-32", new MachineSpec(208.00, 32));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-highmem-64", new MachineSpec(416.00, 64));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-highmem-96", new MachineSpec(624.00, 96));

    // High-CPU (0.90 GB per vCPU)
    STANDARD_MACHINE_TYPE_SPECS.put("n1-highcpu-2", new MachineSpec(1.80, 2));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-highcpu-4", new MachineSpec(3.60, 4));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-highcpu-8", new MachineSpec(7.20, 8));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-highcpu-16", new MachineSpec(14.40, 16));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-highcpu-32", new MachineSpec(28.80, 32));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-highcpu-64", new MachineSpec(57.60, 64));
    STANDARD_MACHINE_TYPE_SPECS.put("n1-highcpu-96", new MachineSpec(86.40, 96));

    // -------------------------------------------------------------------------
    // N2 Series (General Purpose - Modern)
    // Source:
    // https://cloud.google.com/compute/docs/general-purpose-machines#n2_machines
    // -------------------------------------------------------------------------

    // Standard (4.00 GB per vCPU)
    STANDARD_MACHINE_TYPE_SPECS.put("n2-standard-2", new MachineSpec(8.00, 2));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-standard-4", new MachineSpec(16.00, 4));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-standard-8", new MachineSpec(32.00, 8));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-standard-16", new MachineSpec(64.00, 16));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-standard-32", new MachineSpec(128.00, 32));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-standard-48", new MachineSpec(192.00, 48));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-standard-64", new MachineSpec(256.00, 64));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-standard-80", new MachineSpec(320.00, 80));

    // High-Memory (8.00 GB per vCPU)
    STANDARD_MACHINE_TYPE_SPECS.put("n2-highmem-2", new MachineSpec(16.00, 2));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-highmem-4", new MachineSpec(32.00, 4));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-highmem-8", new MachineSpec(64.00, 8));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-highmem-16", new MachineSpec(128.00, 16));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-highmem-32", new MachineSpec(256.00, 32));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-highmem-48", new MachineSpec(384.00, 48));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-highmem-64", new MachineSpec(512.00, 64));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-highmem-80", new MachineSpec(640.00, 80));

    // High-CPU (1.00 GB per vCPU)
    STANDARD_MACHINE_TYPE_SPECS.put("n2-highcpu-2", new MachineSpec(2.00, 2));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-highcpu-4", new MachineSpec(4.00, 4));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-highcpu-8", new MachineSpec(8.00, 8));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-highcpu-16", new MachineSpec(16.00, 16));
    STANDARD_MACHINE_TYPE_SPECS.put("n2-highcpu-32", new MachineSpec(32.00, 32));

    // -------------------------------------------------------------------------
    // E2 Series (Cost-Optimized / Dynamic)
    // Source:
    // https://cloud.google.com/compute/docs/general-purpose-machines#e2_machines
    // -------------------------------------------------------------------------

    // Standard (4.00 GB per vCPU)
    STANDARD_MACHINE_TYPE_SPECS.put("e2-standard-2", new MachineSpec(8.00, 2));
    STANDARD_MACHINE_TYPE_SPECS.put("e2-standard-4", new MachineSpec(16.00, 4));
    STANDARD_MACHINE_TYPE_SPECS.put("e2-standard-8", new MachineSpec(32.00, 8));
    STANDARD_MACHINE_TYPE_SPECS.put("e2-standard-16", new MachineSpec(64.00, 16));
    STANDARD_MACHINE_TYPE_SPECS.put("e2-standard-32", new MachineSpec(128.00, 32));

    // High-Memory (8.00 GB per vCPU)
    STANDARD_MACHINE_TYPE_SPECS.put("e2-highmem-2", new MachineSpec(16.00, 2));
    STANDARD_MACHINE_TYPE_SPECS.put("e2-highmem-4", new MachineSpec(32.00, 4));
    STANDARD_MACHINE_TYPE_SPECS.put("e2-highmem-8", new MachineSpec(64.00, 8));
    STANDARD_MACHINE_TYPE_SPECS.put("e2-highmem-16", new MachineSpec(128.00, 16));

    // -------------------------------------------------------------------------
    // C2 Series (Compute-Optimized)
    // Source:
    // https://cloud.google.com/compute/docs/compute-optimized-machines#c2_machines
    // -------------------------------------------------------------------------

    // Standard (4.00 GB per vCPU)
    STANDARD_MACHINE_TYPE_SPECS.put("c2-standard-4", new MachineSpec(16.00, 4));
    STANDARD_MACHINE_TYPE_SPECS.put("c2-standard-8", new MachineSpec(32.00, 8));
    STANDARD_MACHINE_TYPE_SPECS.put("c2-standard-16", new MachineSpec(64.00, 16));
    STANDARD_MACHINE_TYPE_SPECS.put("c2-standard-30", new MachineSpec(120.00, 30));
    STANDARD_MACHINE_TYPE_SPECS.put("c2-standard-60", new MachineSpec(240.00, 60));

    // -------------------------------------------------------------------------
    // M1 Series (Memory-Optimized)
    // Source: https://cloud.google.com/compute/docs/memory-optimized-machines
    // -------------------------------------------------------------------------

    // Ultra-High Memory
    STANDARD_MACHINE_TYPE_SPECS.put("m1-ultramem-40", new MachineSpec(961.00, 40));
    STANDARD_MACHINE_TYPE_SPECS.put("m1-ultramem-80", new MachineSpec(1922.00, 80));
    STANDARD_MACHINE_TYPE_SPECS.put("m1-ultramem-160", new MachineSpec(3844.00, 160));
    STANDARD_MACHINE_TYPE_SPECS.put("m1-megamem-96", new MachineSpec(1433.60, 96));
  }

  public static Double getWorkerMemoryGB(String workerMachineType) {
    Preconditions.checkArgument(
        workerMachineType != null && !StringUtils.isBlank(workerMachineType),
        "workerMachineType cannot be null or empty.");

    // Handle standard machine types
    MachineSpec spec = STANDARD_MACHINE_TYPE_SPECS.get(workerMachineType);
    if (spec != null) {
      return spec.memoryGB;
    }

    // Handle custom machine types
    spec = tryParseCustomMachineType(workerMachineType);
    if (spec != null) {
      return spec.memoryGB;
    }

    return null;
  }

  public static Integer getWorkerCores(String workerMachineType) {
    Preconditions.checkArgument(
        workerMachineType != null && !StringUtils.isBlank(workerMachineType),
        "workerMachineType cannot be null or empty.");

    // Handle standard machine types
    MachineSpec spec = STANDARD_MACHINE_TYPE_SPECS.get(workerMachineType);
    if (spec != null) {
      return spec.vCPUs;
    }

    // Handle custom machine types
    spec = tryParseCustomMachineType(workerMachineType);
    if (spec != null) {
      return spec.vCPUs;
    }

    return null;
  }

  private static MachineSpec tryParseCustomMachineType(String workerMachineType) {
    // Regex for custom machine types: FAMILY-vCPU-MEMORY[-ext]
    // Supported families: custom, n2-custom, n2d-custom, n4-custom, e2-custom
    // Examples:
    // custom-2-4096 => vCPU=2, Mem=4096MB
    // n2-custom-4-8192 => vCPU=4, Mem=8192MB
    // n2-custom-4-8192-ext => vCPU=4, Mem=8192MB
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
        "^(custom|n2-custom|n2d-custom|n4-custom|e2-custom)-(\\d+)-(\\d+)(?:-ext)?$");
    java.util.regex.Matcher matcher = pattern.matcher(workerMachineType);

    if (matcher.matches()) {
      try {
        int vCPUs = Integer.parseInt(matcher.group(2));
        long memoryMB = Long.parseLong(matcher.group(3));
        double memoryGB = memoryMB / 1024.0;
        return new MachineSpec(memoryGB, vCPUs);
      } catch (NumberFormatException e) {
        // Should be caught by regex, but safe to ignore
        return null;
      }
    }
    return null;
  }

  private static class MachineSpec {
    final double memoryGB;
    final int vCPUs;

    MachineSpec(double memoryGB, int vCPUs) {
      this.memoryGB = memoryGB;
      this.vCPUs = vCPUs;
    }
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
