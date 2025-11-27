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
        assertThrows(IllegalArgumentException.class, () -> {
            DataflowWorkerMachineTypeValidator.validateMachineSpecs("n1-standard-2", 4);
        });
    }

    @Test
    public void testNullMachineType() {
        assertThrows(IllegalArgumentException.class, () -> {
            DataflowWorkerMachineTypeValidator.validateMachineSpecs(null, 4);
        });
    }

    @Test
    public void testEmptyMachineType() {
        assertThrows(IllegalArgumentException.class, () -> {
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
        assertThrows(IllegalArgumentException.class, () -> {
            DataflowWorkerMachineTypeValidator.validateMachineSpecs("custom-2-12345", 4);
        });
    }

    @Test
    public void testInvalidCustomMachineTypeFormat() {
        assertThrows(IllegalArgumentException.class, () -> {
            DataflowWorkerMachineTypeValidator.validateMachineSpecs("custom-2", 4);
        });
    }

    @Test
    public void testInvalidCustomMachineTypeNonNumericCpu() {
        assertThrows(IllegalArgumentException.class, () -> {
            DataflowWorkerMachineTypeValidator.validateMachineSpecs("custom-abc-12345", 4);
        });
    }

    @Test
    public void testUnknownMachineType() {
        assertThrows(IllegalArgumentException.class, () -> {
            DataflowWorkerMachineTypeValidator.validateMachineSpecs("unknown-machine-type", 4);
        });
    }
}
