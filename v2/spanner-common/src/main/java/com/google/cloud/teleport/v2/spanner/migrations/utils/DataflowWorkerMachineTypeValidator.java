package com.google.cloud.teleport.v2.spanner.migrations.utils;

public class DataflowWorkerMachineTypeValidator {

  public static void validateMachineSpecs(String workerMachineType, Integer minCPUs) {
    if (workerMachineType == null || workerMachineType.trim().isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Policy Violation: You must specify a workerMachineType with at least %d vCPUs.",
              minCPUs));
    }

    // Handle custom machine types first, format is custom-{vCPU}-{RAM}
    if (workerMachineType.startsWith("custom-")) {
      String[] parts = workerMachineType.split("-");
      if (parts.length != 3) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid custom machine type format: '%s'. Expected format: custom-{vCPU}-{RAM}.",
                workerMachineType));
      }
      try {
        int vCpus = Integer.parseInt(parts[1]);
        if (vCpus < minCPUs) {
          throw new IllegalArgumentException(
              String.format(
                  "Policy Violation: Custom machine type '%s' has %d vCPUs. Minimum allowed is %d. Please use a higher machine type.",
                  workerMachineType, vCpus, minCPUs));
        }
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            String.format("Invalid vCPU number in custom machine type: '%s'", workerMachineType),
            e);
      }
    } else {
      // Handle standard machine types.
      java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(".*-(\\d+)$");
      java.util.regex.Matcher matcher = pattern.matcher(workerMachineType);

      if (matcher.find()) {
        try {
          int vCpus = Integer.parseInt(matcher.group(1));
          if (vCpus < minCPUs) {
            throw new IllegalArgumentException(
                String.format(
                    "Policy Violation: Machine type '%s' has %d vCPUs. Minimum allowed is %d.",
                    workerMachineType, vCpus, minCPUs));
          }
        } catch (NumberFormatException e) {
          // This should be rare given the regex, but good practice to handle.
          throw new IllegalArgumentException(
              String.format("Invalid vCPU number in machine type: '%s'", workerMachineType), e);
        }
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Unknown machine type format: '%s'. Please use a standard machine type (e.g., n1-standard-4) or a custom machine type (e.g., custom-4-4096) with at least %d vCPUs.",
                workerMachineType, minCPUs));
      }
    }
  }

}
