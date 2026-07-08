/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.dofn;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class BuildSchemaDagFnTest {

  @Test
  public void testProcessElement_BuildsDAG() {
    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("table1")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of("table1", table)).build();

    BuildSchemaDagFn fn = new BuildSchemaDagFn();
    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    ArgumentCaptor<DataGeneratorSchema> captor = ArgumentCaptor.forClass(DataGeneratorSchema.class);

    fn.processElement(schema, receiver);

    verify(receiver).output(captor.capture());
    DataGeneratorSchema resolvedSchema = captor.getValue();
    assertNotNull(resolvedSchema);
    DataGeneratorTable resolvedTable = resolvedSchema.tables().get("table1");
    assertNotNull(resolvedTable);
    assertTrue(resolvedTable.isRoot()); // It has no FKs, so it should be root
  }
}
