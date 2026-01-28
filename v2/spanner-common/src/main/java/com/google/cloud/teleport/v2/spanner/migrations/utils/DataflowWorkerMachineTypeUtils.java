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

import com.google.cloud.compute.v1.MachineType;
import com.google.cloud.compute.v1.MachineTypesClient;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataflowWorkerMachineTypeUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DataflowWorkerMachineTypeUtils.class);
  private static final Map<String, MachineSpec> MACHINE_SPEC_CACHE = new ConcurrentHashMap<>();
  private static final String DEFAULT_ZONE = "us-central1-a";

  public static Double getWorkerMemoryGB(String workerMachineType) {
    return getWorkerMemoryGB(null, null, workerMachineType);
  }

  public static Double getWorkerMemoryGB(String projectId, String zone, String workerMachineType) {
    MachineSpec spec = getMachineSpec(projectId, zone, workerMachineType);
    return spec != null ? spec.memoryGB : null;
  }

  public static Integer getWorkerCores(String workerMachineType) {
    return getWorkerCores(null, null, workerMachineType);
  }

  public static Integer getWorkerCores(String projectId, String zone, String workerMachineType) {
    MachineSpec spec = getMachineSpec(projectId, zone, workerMachineType);
    return spec != null ? spec.vCPUs : null;
  }

  private static MachineSpec getMachineSpec(
      String projectId, String zone, String workerMachineType) {
    Preconditions.checkArgument(
        workerMachineType != null && !StringUtils.isBlank(workerMachineType),
        "workerMachineType cannot be null or empty.");

    // Check cache first
    if (MACHINE_SPEC_CACHE.containsKey(workerMachineType)) {
      return MACHINE_SPEC_CACHE.get(workerMachineType);
    }

    // Fetch from Compute Engine API
    MachineSpec apiSpec = fetchMachineSpecFromApi(projectId, zone, workerMachineType);
    if (apiSpec != null) {
      MACHINE_SPEC_CACHE.put(workerMachineType, apiSpec);
      return apiSpec;
    }

    return null;
  }

  private static MachineSpec fetchMachineSpecFromApi(
      String projectId, String zone, String workerMachineType) {
    if (zone == null) {
      LOG.warn("Could not determine Zone. Defaulting to {}.", DEFAULT_ZONE);
      zone = DEFAULT_ZONE;
    }

    if (projectId == null) {
      LOG.warn("Could not determine Project ID. Cannot fetch machine type details from API.");
      return null;
    }

    try (MachineTypesClient client = MachineTypesClient.create()) {
      // machineTypes.get() returns the resource or throws NotFoundException
      // API documentation confirms custom types like custom-CPUS-MEM are supported
      MachineType machineType = client.get(projectId, zone, workerMachineType);

      // machineType.getMemoryMb() is int, returns memory in MB
      // machineType.getGuestCpus() is int
      double memoryGB = machineType.getMemoryMb() / 1024.0;
      int vCPUs = machineType.getGuestCpus();

      LOG.info(
          "Fetched machine type {} from API: {} vCPUs, {} GB RAM",
          workerMachineType,
          vCPUs,
          memoryGB);

      return new MachineSpec(memoryGB, vCPUs);
    } catch (Exception e) {
      LOG.warn(
          "Failed to fetch machine type '{}' from Compute Engine API (Project: {}, Zone: {}): {}",
          workerMachineType,
          projectId,
          DEFAULT_ZONE,
          e.getMessage());
      return null;
    }
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

  @com.google.common.annotations.VisibleForTesting
  static void putMachineSpecForTesting(String machineType, double memoryGB, int vCPUs) {
    MACHINE_SPEC_CACHE.put(machineType, new MachineSpec(memoryGB, vCPUs));
  }

  @com.google.common.annotations.VisibleForTesting
  static void resetCacheForTesting() {
    MACHINE_SPEC_CACHE.clear();
  }
}
