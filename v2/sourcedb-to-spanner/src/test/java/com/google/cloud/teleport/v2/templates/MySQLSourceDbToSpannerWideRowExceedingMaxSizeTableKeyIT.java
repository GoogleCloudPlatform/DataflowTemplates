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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.spanner.Mutation;
import java.util.List;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MySQLSourceDbToSpannerWideRowExceedingMaxSizeTableKeyIT
    extends SourceDbToSpannerITBase {
  private static final String SPANNER_SCHEMA_EXCEEDING_TABLE_KEY_FILE_RESOURCE =
      "WideRow/SourceDbToSpannerMaxSizeTableKey/spanner-schema-exceeding-tablekey.sql";

  public static SpannerResourceManager spannerResourceManager;

  @Before
  public void setUp() throws Exception {
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void cleanUp() throws Exception {
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  //   This test is expected to fail because the key size exceeds the maximum allowed size.
  public void insertExceedingMaxSizeTableKey() throws Exception {
    String tableName = "TestKeyComposite";
    List<Mutation> mutations =
        List.of(
            Mutation.newInsertBuilder(tableName)
                .set("pk1")
                .to("a".repeat(4000))
                .set("pk2")
                .to("b".repeat(4000))
                .set("value")
                .to("Valid composite key size.")
                .build(),
            Mutation.newInsertBuilder(tableName)
                .set("pk1")
                .to("a".repeat(5000))
                .set("pk2")
                .to("b".repeat(5000))
                .set("value")
                .to("Exceeds key size limit.")
                .build());
    spannerResourceManager.write(mutations);
  }

  @Test
  public void wideRowExceedingMaxSizeTableKey() throws Exception {
    try {
      createSpannerDDL(spannerResourceManager, SPANNER_SCHEMA_EXCEEDING_TABLE_KEY_FILE_RESOURCE);
      insertExceedingMaxSizeTableKey();
    } catch (Exception e) {
      System.out.println("===>>>>>> Exception caught: " + e.getMessage());
      Assert.assertTrue(
          "Exception should mention max size table key limitation",
          e.getMessage().contains("Failed to write mutations"));
    }
  }
}
