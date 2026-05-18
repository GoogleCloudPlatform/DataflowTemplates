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
package com.google.cloud.teleport.v2.templates.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.dofn.FetchSchemaFn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.sink.SinkSchemaFetcher;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SchemaLoader}. */
@RunWith(JUnit4.class)
public class SchemaLoaderTest {

  @Test
  public void testFetchSchemaFn_Spanner() throws IOException {
    final SinkSchemaFetcher mockFetcher = mock(SinkSchemaFetcher.class);
    final DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of()).build();

    when(mockFetcher.getSchema()).thenReturn(schema);

    FetchSchemaFn fn =
        new FetchSchemaFn(SinkType.SPANNER, "options") {
          @Override
          protected SinkSchemaFetcher createFetcher(SinkType sinkType) {
            assertEquals(SinkType.SPANNER, sinkType);
            return mockFetcher;
          }
        };

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    fn.processElement(receiver);
    verify(mockFetcher).init("options");
    verify(receiver).output(schema);
  }

  @Test
  public void testFetchSchemaFn_MySql() throws IOException {
    final SinkSchemaFetcher mockFetcher = mock(SinkSchemaFetcher.class);
    final DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of()).build();
    when(mockFetcher.getSchema()).thenReturn(schema);

    FetchSchemaFn fn =
        new FetchSchemaFn(SinkType.MYSQL, "options") {
          @Override
          protected SinkSchemaFetcher createFetcher(SinkType sinkType) {
            assertEquals(SinkType.MYSQL, sinkType);
            return mockFetcher;
          }
        };

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    fn.processElement(receiver);
    verify(mockFetcher).init("options");
    verify(receiver).output(schema);
  }

  @Test
  public void testFetchSchemaFn_Unsupported() throws IOException {
    FetchSchemaFn fn = new FetchSchemaFn(null, "options");

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    assertThrows(IllegalArgumentException.class, () -> fn.processElement(receiver));
  }

  @Test
  public void testFetchSchemaFn_IOException() throws IOException {
    final SinkSchemaFetcher mockFetcher = mock(SinkSchemaFetcher.class);
    when(mockFetcher.getSchema()).thenThrow(new IOException("File not found"));

    FetchSchemaFn fn =
        new FetchSchemaFn(SinkType.SPANNER, "options") {
          @Override
          protected SinkSchemaFetcher createFetcher(SinkType sinkType) {
            return mockFetcher;
          }
        };

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    assertThrows(RuntimeException.class, () -> fn.processElement(receiver));
  }
}
