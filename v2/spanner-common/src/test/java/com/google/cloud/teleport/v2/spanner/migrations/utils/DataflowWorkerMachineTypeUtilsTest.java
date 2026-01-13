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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataflowWorkerMachineTypeUtilsTest {

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
    assertNull(DataflowWorkerMachineTypeUtils.getWorkerMemoryGB("unknown-machine"));
    assertNull(DataflowWorkerMachineTypeUtils.getWorkerMemoryGB("custom-2-invalid"));

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DataflowWorkerMachineTypeUtils.getWorkerMemoryGB(null);
        });
  }
}
