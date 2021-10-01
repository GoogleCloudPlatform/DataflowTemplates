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
package com.google.cloud.teleport.v2.values;

import static com.google.common.truth.Truth.assertThat;

import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BigQueryTable}. */
@RunWith(JUnit4.class)
public class BigQueryTableTest {
  @Test
  @Category(NeedsRunner.class)
  public void testDeserializedObjectEqualsSerialized() throws Exception {
    BigQueryTable expected =
        BigQueryTable.builder()
            .setTableName("t1")
            .setProject("proj1")
            .setDataset("ds1")
            .setLastModificationTime(123L)
            .setSchema(Schema.create(Schema.Type.STRING))
            .setPartitioningColumn("p1")
            .setPartitions(
                Collections.singletonList(
                    BigQueryTablePartition.builder()
                        .setPartitionName("ppp1")
                        .setLastModificationTime(234L)
                        .build()))
            .build();

    BigQueryTable actual = (BigQueryTable) SerializationUtils.clone(expected);

    assertThat(actual).isEqualTo(expected);
  }
}
