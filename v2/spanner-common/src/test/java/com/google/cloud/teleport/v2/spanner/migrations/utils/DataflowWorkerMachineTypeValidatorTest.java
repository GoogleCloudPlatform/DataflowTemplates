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

import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataflowWorkerMachineTypeValidatorTest {

  @Test
  public void testValidMachineType() {
    DataflowWorkerMachineTypeValidator.validateMachineSpecs("n1-standard-4", 4);
  }

  @Test
  public void testValidMachineTypeHighCpu() {
    DataflowWorkerMachineTypeValidator.validateMachineSpecs("n1-standard-8", 4);
  }

  @Test
  public void testInvalidMachineTypeLowCpu() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DataflowWorkerMachineTypeValidator.validateMachineSpecs("n1-standard-2", 4);
        });
  }

  @Test
  public void testNullMachineType() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DataflowWorkerMachineTypeValidator.validateMachineSpecs(null, 4);
        });
  }

  @Test
  public void testEmptyMachineType() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DataflowWorkerMachineTypeValidator.validateMachineSpecs(" ", 4);
        });
  }

  @Test
  public void testValidCustomMachineType() {
    DataflowWorkerMachineTypeValidator.validateMachineSpecs("custom-8-12345", 4);
  }

  @Test
  public void testValidCustomMachineTypeMinCpu() {
    DataflowWorkerMachineTypeValidator.validateMachineSpecs("custom-4-12345", 4);
  }

  @Test
  public void testInvalidCustomMachineTypeLowCpu() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DataflowWorkerMachineTypeValidator.validateMachineSpecs("custom-2-12345", 4);
        });
  }

  @Test
  public void testInvalidCustomMachineTypeFormat() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DataflowWorkerMachineTypeValidator.validateMachineSpecs("custom-2", 4);
        });
  }

  @Test
  public void testInvalidCustomMachineTypeNonNumericCpu() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DataflowWorkerMachineTypeValidator.validateMachineSpecs("custom-abc-12345", 4);
        });
  }

  @Test
  public void testUnknownMachineType() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DataflowWorkerMachineTypeValidator.validateMachineSpecs("unknown-machine-type", 4);
        });
  }
}
