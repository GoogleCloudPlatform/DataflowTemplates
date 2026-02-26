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
package com.google.cloud.teleport.v2.dofn;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class CreateSpannerReadOpsFnTest {

  @Test
  public void testProcessElement() {
    // Mock dependencies
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn<Void, ReadOperation>.ProcessContext context = mock(DoFn.ProcessContext.class);
    Ddl ddl = mock(Ddl.class);

    // Prepare DDL behavior
    when(ddl.getTablesOrderedByReference()).thenReturn(ImmutableList.of("Table1", "Table2"));

    // Define context behavior
    when(context.sideInput(ddlView)).thenReturn(ddl);

    // Create DoFn
    CreateSpannerReadOpsFn doFn = new CreateSpannerReadOpsFn(ddlView);

    // Execute
    doFn.processElement(context);

    // Verify output
    ArgumentCaptor<ReadOperation> argument = ArgumentCaptor.forClass(ReadOperation.class);
    verify(context, times(2)).output(argument.capture());

    // Validate captured arguments
    verify(context)
        .output(
            ReadOperation.create().withQuery("SELECT *, 'Table1' as __tableName__ FROM Table1"));
    verify(context)
        .output(
            ReadOperation.create().withQuery("SELECT *, 'Table2' as __tableName__ FROM Table2"));
  }
}
