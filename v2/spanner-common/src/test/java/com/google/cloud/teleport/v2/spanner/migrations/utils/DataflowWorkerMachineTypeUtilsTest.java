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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.cloud.compute.v1.MachineType;
import com.google.cloud.compute.v1.MachineTypesClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockedStatic;

@RunWith(JUnit4.class)
public class DataflowWorkerMachineTypeUtilsTest {

  @Before
  public void setUp() {
    DataflowWorkerMachineTypeUtils.resetCacheForTesting();
    // Pre-populate cache to avoid API calls during tests

    // Standard types
    DataflowWorkerMachineTypeUtils.putMachineSpecForTesting("n1-standard-4", 15.00, 4);
    DataflowWorkerMachineTypeUtils.putMachineSpecForTesting("n1-standard-8", 30.00, 8);
    DataflowWorkerMachineTypeUtils.putMachineSpecForTesting("n1-standard-96", 360.00, 96);
    DataflowWorkerMachineTypeUtils.putMachineSpecForTesting("n1-highmem-8", 52.00, 8);

    // Custom types used in tests
    DataflowWorkerMachineTypeUtils.putMachineSpecForTesting("custom-2-4096", 4.0, 2);
    DataflowWorkerMachineTypeUtils.putMachineSpecForTesting("n2-custom-4-8192", 8.0, 4);
    DataflowWorkerMachineTypeUtils.putMachineSpecForTesting("n2d-custom-2-2048", 2.0, 2);
    DataflowWorkerMachineTypeUtils.putMachineSpecForTesting("e2-custom-2-4096", 4.0, 2);
    DataflowWorkerMachineTypeUtils.putMachineSpecForTesting("n4-custom-32-131072", 128.0, 32);
    DataflowWorkerMachineTypeUtils.putMachineSpecForTesting("n2-custom-4-8192-ext", 8.0, 4);
  }

  @Test
  public void testValidMachineType() {
    DataflowWorkerMachineTypeUtils.validateMachineSpecs("n1-standard-4", 4);
  }

  @Test
  public void testValidMachineTypeHighCpu() {
    DataflowWorkerMachineTypeUtils.validateMachineSpecs("n1-standard-8", 4);
  }

  @Test
  public void testInvalidMachineTypeLowCpu() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DataflowWorkerMachineTypeUtils.validateMachineSpecs("n1-standard-2", 4);
        });
  }

  @Test
  public void testNullMachineType() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DataflowWorkerMachineTypeUtils.validateMachineSpecs(null, 4);
        });
  }

  @Test
  public void testEmptyMachineType() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DataflowWorkerMachineTypeUtils.validateMachineSpecs(" ", 4);
        });
  }

  @Test
  public void testValidCustomMachineType() {
    DataflowWorkerMachineTypeUtils.validateMachineSpecs("custom-8-12345", 4);
  }

  @Test
  public void testValidCustomMachineTypeMinCpu() {
    DataflowWorkerMachineTypeUtils.validateMachineSpecs("custom-4-12345", 4);
  }

  @Test
  public void testInvalidCustomMachineTypeLowCpu() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DataflowWorkerMachineTypeUtils.validateMachineSpecs("custom-2-12345", 4);
        });
  }

  @Test
  public void testInvalidCustomMachineTypeFormat() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DataflowWorkerMachineTypeUtils.validateMachineSpecs("custom-2", 4);
        });
  }

  @Test
  public void testInvalidCustomMachineTypeNonNumericCpu() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DataflowWorkerMachineTypeUtils.validateMachineSpecs("custom-abc-12345", 4);
        });
  }

  @Test
  public void testUnknownMachineType() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DataflowWorkerMachineTypeUtils.validateMachineSpecs("unknown-machine-type", 4);
        });
  }

  @Test
  public void testGetWorkerMemoryGBStandard() {
    assertEquals(
        15.00,
        DataflowWorkerMachineTypeUtils.getWorkerMemoryGB(null, null, "n1-standard-4"),
        0.001);
    assertEquals(
        52.00, DataflowWorkerMachineTypeUtils.getWorkerMemoryGB(null, null, "n1-highmem-8"), 0.001);
  }

  @Test
  public void testGetWorkerMemoryGBInvalid() {
    // This will try to fetch from API and fail (return null) because it's not in
    // cache and invalid/unknown project
    assertNull(DataflowWorkerMachineTypeUtils.getWorkerMemoryGB(null, null, "unknown-machine"));

    // Custom types with invalid structure or not matching regex should return null
    // or throw depending on usage
    // logic in getWorkerMemoryGB checks getMachineSpec which checks
    // tryParseCustomMachineType
    assertNull(DataflowWorkerMachineTypeUtils.getWorkerMemoryGB(null, null, "custom-2-invalid"));

    IllegalArgumentException exception1 =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              DataflowWorkerMachineTypeUtils.getWorkerMemoryGB(null, null, null);
            });
    assertEquals("workerMachineType cannot be null or empty.", exception1.getMessage());

    IllegalArgumentException exception2 =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              DataflowWorkerMachineTypeUtils.getWorkerMemoryGB(null, null, "");
            });
    assertEquals("workerMachineType cannot be null or empty.", exception2.getMessage());

    IllegalArgumentException exception3 =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              DataflowWorkerMachineTypeUtils.getWorkerMemoryGB(null, null, "  ");
            });
    assertEquals("workerMachineType cannot be null or empty.", exception3.getMessage());
  }

  @Test
  public void testGetWorkerCoresStandard() {
    assertEquals(
        4, (int) DataflowWorkerMachineTypeUtils.getWorkerCores(null, null, "n1-standard-4"));
    assertEquals(
        8, (int) DataflowWorkerMachineTypeUtils.getWorkerCores(null, null, "n1-highmem-8"));
    assertEquals(
        96, (int) DataflowWorkerMachineTypeUtils.getWorkerCores(null, null, "n1-standard-96"));
  }

  @Test
  public void testGetWorkerCoresInvalid() {
    assertNull(DataflowWorkerMachineTypeUtils.getWorkerCores(null, null, "unknown-machine"));
    assertNull(
        DataflowWorkerMachineTypeUtils.getWorkerCores(
            null, null, "custom-2-invalid")); // Invalid parsing
    assertNull(
        DataflowWorkerMachineTypeUtils.getWorkerCores(
            null, null, "invalid-custom-2-1024")); // Invalid family

    IllegalArgumentException exception1 =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              DataflowWorkerMachineTypeUtils.getWorkerCores(null, null, null);
            });
    assertEquals("workerMachineType cannot be null or empty.", exception1.getMessage());

    IllegalArgumentException exception2 =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              DataflowWorkerMachineTypeUtils.getWorkerCores(null, null, "");
            });
    assertEquals("workerMachineType cannot be null or empty.", exception2.getMessage());

    IllegalArgumentException exception3 =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              DataflowWorkerMachineTypeUtils.getWorkerCores(null, null, "  ");
            });
    assertEquals("workerMachineType cannot be null or empty.", exception3.getMessage());
  }

  @Test
  public void testGetWorkerMemoryGBCustom() {
    // custom-2-4096 => 4096MB = 4GB
    assertEquals(
        4.0, DataflowWorkerMachineTypeUtils.getWorkerMemoryGB(null, null, "custom-2-4096"), 0.001);
    // n2-custom-4-8192 => 8192MB = 8GB
    assertEquals(
        8.0,
        DataflowWorkerMachineTypeUtils.getWorkerMemoryGB(null, null, "n2-custom-4-8192"),
        0.001);
    // n2d-custom-2-2048 => 2048MB = 2GB
    assertEquals(
        2.0,
        DataflowWorkerMachineTypeUtils.getWorkerMemoryGB(null, null, "n2d-custom-2-2048"),
        0.001);
    // e2-custom-2-4096 => 4096MB = 4GB
    assertEquals(
        4.0,
        DataflowWorkerMachineTypeUtils.getWorkerMemoryGB(null, null, "e2-custom-2-4096"),
        0.001);
    // n4-custom-32-131072 => 128GB
    assertEquals(
        128.0,
        DataflowWorkerMachineTypeUtils.getWorkerMemoryGB(null, null, "n4-custom-32-131072"),
        0.001);
    // extended memory
    assertEquals(
        8.0,
        DataflowWorkerMachineTypeUtils.getWorkerMemoryGB(null, null, "n2-custom-4-8192-ext"),
        0.001);
  }

  @Test
  public void testGetWorkerCoresCustom() {
    assertEquals(
        2, (int) DataflowWorkerMachineTypeUtils.getWorkerCores(null, null, "custom-2-4096"));
    assertEquals(
        4, (int) DataflowWorkerMachineTypeUtils.getWorkerCores(null, null, "n2-custom-4-8192"));
    assertEquals(
        2, (int) DataflowWorkerMachineTypeUtils.getWorkerCores(null, null, "n2d-custom-2-2048"));
    assertEquals(
        32, (int) DataflowWorkerMachineTypeUtils.getWorkerCores(null, null, "n4-custom-32-131072"));
  }

  @Test
  public void testFetchMachineSpecFromApi_Success() {
    String projectId = "test-project";
    String zone = "us-central1-a";
    String machineType = "n1-standard-new"; // Not in pre-populated cache

    try (MockedStatic<MachineTypesClient> mockedStaticClient =
        mockStatic(MachineTypesClient.class)) {
      MachineTypesClient mockClient = mock(MachineTypesClient.class);
      mockedStaticClient.when(MachineTypesClient::create).thenReturn(mockClient);

      MachineType mockMachineType = mock(MachineType.class);
      when(mockMachineType.getGuestCpus()).thenReturn(4);
      when(mockMachineType.getMemoryMb()).thenReturn(15360); // 15 GB

      when(mockClient.get(anyString(), anyString(), anyString())).thenReturn(mockMachineType);

      Double memoryGB =
          DataflowWorkerMachineTypeUtils.getWorkerMemoryGB(projectId, zone, machineType);
      Integer vCpus = DataflowWorkerMachineTypeUtils.getWorkerCores(projectId, zone, machineType);

      assertEquals(15.0, memoryGB, 0.001);
      assertEquals(4, (int) vCpus);
    }
  }

  @Test
  public void testFetchMachineSpecFromApi_Failure() {
    String projectId = "test-project";
    String zone = "us-central1-a";
    String machineType = "unknown-machine";

    try (MockedStatic<MachineTypesClient> mockedStaticClient =
        mockStatic(MachineTypesClient.class)) {
      MachineTypesClient mockClient = mock(MachineTypesClient.class);
      mockedStaticClient.when(MachineTypesClient::create).thenReturn(mockClient);

      when(mockClient.get(anyString(), anyString(), anyString()))
          .thenThrow(new RuntimeException("API Error"));

      Double memoryGB =
          DataflowWorkerMachineTypeUtils.getWorkerMemoryGB(projectId, zone, machineType);
      Integer vCpus = DataflowWorkerMachineTypeUtils.getWorkerCores(projectId, zone, machineType);

      assertNull(memoryGB);
      assertNull(vCpus);
    }
  }
}
