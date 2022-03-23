/*
 * Copyright (C) 2022 Google LLC
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

import static com.google.common.truth.Truth.assertThat;

import java.io.File;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link StorageUtils}. */
@RunWith(JUnit4.class)
public final class StorageUtilsTest {
  @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();

  @Test
  public void testGetFilesInDirectory_withValidPath_returnsPathsOfFilesInDirectory()
      throws Exception {
    File outDir = tmpDir.newFolder("out");
    File outputDir1 = tmpDir.newFolder("out", "unpartitioned_table");
    File outputFile1 =
        new File(outputDir1.getAbsolutePath() + "/" + "output-unpartitioned_table.parquet");
    outputFile1.createNewFile();
    File outputDir2 = tmpDir.newFolder("out", "partitioned_table", "p2_pid=partition");
    File outputFile2 =
        new File(outputDir2.getAbsolutePath() + "/" + "output-partitioned_table-partition.parquet");
    outputFile2.createNewFile();

    List<String> files = StorageUtils.getFilesInDirectory(outDir.getAbsolutePath());
    assertThat(files.size()).isEqualTo(2);
  }
}
