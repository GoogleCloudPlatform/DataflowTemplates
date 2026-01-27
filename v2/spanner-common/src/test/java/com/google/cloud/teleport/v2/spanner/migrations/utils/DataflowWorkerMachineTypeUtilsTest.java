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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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
    assertEquals(15.00, DataflowWorkerMachineTypeUtils.getWorkerMemoryGB("n1-standard-4"), 0.001);
    assertEquals(52.00, DataflowWorkerMachineTypeUtils.getWorkerMemoryGB("n1-highmem-8"), 0.001);
  }

  @Test
  public void testGetWorkerMemoryGBInvalid() {
    // This will try to fetch from API and fail (return null) because it's not in
    // cache and invalid/unknown project
    assertNull(DataflowWorkerMachineTypeUtils.getWorkerMemoryGB("unknown-machine"));

    // Custom types with invalid structure or not matching regex should return null
    // or throw depending on usage
    // logic in getWorkerMemoryGB checks getMachineSpec which checks
    // tryParseCustomMachineType
    assertNull(DataflowWorkerMachineTypeUtils.getWorkerMemoryGB("custom-2-invalid"));

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DataflowWorkerMachineTypeUtils.getWorkerMemoryGB(null);
        });
  }

  @Test
  public void testGetWorkerCoresStandard() {
    assertEquals(4, (int) DataflowWorkerMachineTypeUtils.getWorkerCores("n1-standard-4"));
    assertEquals(8, (int) DataflowWorkerMachineTypeUtils.getWorkerCores("n1-highmem-8"));
    assertEquals(96, (int) DataflowWorkerMachineTypeUtils.getWorkerCores("n1-standard-96"));
  }

  @Test
  public void testGetWorkerCoresInvalid() {
    assertNull(DataflowWorkerMachineTypeUtils.getWorkerCores("unknown-machine"));
    assertNull(
        DataflowWorkerMachineTypeUtils.getWorkerCores("custom-2-invalid")); // Invalid parsing
    assertNull(
        DataflowWorkerMachineTypeUtils.getWorkerCores("invalid-custom-2-1024")); // Invalid family

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DataflowWorkerMachineTypeUtils.getWorkerCores(null);
        });
  }

  @Test
  public void testGetWorkerMemoryGBCustom() {
    // custom-2-4096 => 4096MB = 4GB
    assertEquals(4.0, DataflowWorkerMachineTypeUtils.getWorkerMemoryGB("custom-2-4096"), 0.001);
    // n2-custom-4-8192 => 8192MB = 8GB
    assertEquals(8.0, DataflowWorkerMachineTypeUtils.getWorkerMemoryGB("n2-custom-4-8192"), 0.001);
    // n2d-custom-2-2048 => 2048MB = 2GB
    assertEquals(2.0, DataflowWorkerMachineTypeUtils.getWorkerMemoryGB("n2d-custom-2-2048"), 0.001);
    // e2-custom-2-4096 => 4096MB = 4GB
    assertEquals(4.0, DataflowWorkerMachineTypeUtils.getWorkerMemoryGB("e2-custom-2-4096"), 0.001);
    // n4-custom-32-131072 => 128GB
    assertEquals(
        128.0, DataflowWorkerMachineTypeUtils.getWorkerMemoryGB("n4-custom-32-131072"), 0.001);
    // extended memory
    assertEquals(
        8.0, DataflowWorkerMachineTypeUtils.getWorkerMemoryGB("n2-custom-4-8192-ext"), 0.001);
  }

  @Test
  public void testGetWorkerCoresCustom() {
    assertEquals(2, (int) DataflowWorkerMachineTypeUtils.getWorkerCores("custom-2-4096"));
    assertEquals(4, (int) DataflowWorkerMachineTypeUtils.getWorkerCores("n2-custom-4-8192"));
    assertEquals(2, (int) DataflowWorkerMachineTypeUtils.getWorkerCores("n2d-custom-2-2048"));
    assertEquals(32, (int) DataflowWorkerMachineTypeUtils.getWorkerCores("n4-custom-32-131072"));
  }
}
