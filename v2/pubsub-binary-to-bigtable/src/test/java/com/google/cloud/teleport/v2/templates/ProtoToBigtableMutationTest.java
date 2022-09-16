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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.templates.BigtableAvroProtoTestUtils.addProtoCell;
import static com.google.cloud.teleport.v2.templates.BigtableAvroProtoTestUtils.assertEquals;
import static com.google.cloud.teleport.v2.templates.BigtableAvroProtoTestUtils.createProtoRow;

import com.google.cloud.teleport.v2.proto.BigtableRow;
import com.google.cloud.teleport.v2.transforms.ProtoToBigtableMutation;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for helper methods for {@link ProtoToBigtableMutation}. */
@RunWith(JUnit4.class)
public final class ProtoToBigtableMutationTest {

  @Test
  public void applyProtoToBigtableFn() {
    BigtableRow.Builder protoRowBuilder1 = createProtoRow("row1");
    addProtoCell(protoRowBuilder1, "family1", "column1", 1, "value1");
    addProtoCell(protoRowBuilder1, "family1", "column1", 2, "value2");
    addProtoCell(protoRowBuilder1, "family1", "column2", 1, "value3");
    addProtoCell(protoRowBuilder1, "family2", "column1", 1, "value4");

    BigtableRow.Builder protoRowBuilder2 = createProtoRow("row2");
    addProtoCell(protoRowBuilder2, "family2", "column2", 2, "value2");

    Mutation rowMutations1 =
        new Put("row1".getBytes())
            .addColumn("family1".getBytes(), "column1".getBytes(), 1, "value1".getBytes())
            .addColumn("family1".getBytes(), "column1".getBytes(), 2, "value2".getBytes())
            .addColumn("family1".getBytes(), "column2".getBytes(), 1, "value3".getBytes())
            .addColumn("family2".getBytes(), "column1".getBytes(), 1, "value4".getBytes());

    Put rowMutations2 = new Put("row2".getBytes());
    rowMutations2.addColumn("family2".getBytes(), "column2".getBytes(), 2, "value2".getBytes());

    ProtoToBigtableMutation protoToBigtableMutation = new ProtoToBigtableMutation();

    assertEquals(rowMutations1, protoToBigtableMutation.convert(protoRowBuilder1.build()));
    assertEquals(rowMutations2, protoToBigtableMutation.convert(protoRowBuilder2.build()));
  }
}
