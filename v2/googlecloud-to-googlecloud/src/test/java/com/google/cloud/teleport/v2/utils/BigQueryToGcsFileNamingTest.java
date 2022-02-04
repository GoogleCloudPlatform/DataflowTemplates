/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BigQueryToGcsFileNaming}. */
@RunWith(JUnit4.class)
public class BigQueryToGcsFileNamingTest {

  @Before
  public void setUp() {}

  @Test
  public void testGetFilenameWithoutPartition() {
    String tableName = "people";
    String suffix = ".avro";
    String expectedFilename = "output-people.avro";
    BigQueryToGcsFileNaming fileNaming = new BigQueryToGcsFileNaming(suffix, tableName);
    assertEquals(fileNaming.getFilename(null, null, 0, 0, null), expectedFilename);
  }

  @Test
  public void testGetFilenameWithPartition() {
    String tableName = "people";
    String partitionName = "name_partition";
    String suffix = ".avro";
    String expectedFilename = "output-people-name_partition.avro";
    BigQueryToGcsFileNaming fileNaming =
        new BigQueryToGcsFileNaming(suffix, tableName, partitionName);
    assertEquals(fileNaming.getFilename(null, null, 0, 0, null), expectedFilename);
  }
}
