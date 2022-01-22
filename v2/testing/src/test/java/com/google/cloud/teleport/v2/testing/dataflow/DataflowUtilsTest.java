package com.google.cloud.teleport.v2.testing.dataflow;

import static com.google.cloud.teleport.v2.testing.dataflow.DataflowUtils.createJobName;
import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataflowUtilsTest {

  @Test
  public void testCreateJobName() {
    String name = "create-job-name";
    assertThat(createJobName(name)).matches(name + "-\\d{14}");
  }
}