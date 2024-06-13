/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.transformer;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.constants.SourceDbToSpannerConstants;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaTestUtils;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.RowContext;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

public class SourceRowToMutationDoFnTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testSourceRowToMutationDoFn() {
    final String testTable = "srcTable";
    var schema = SchemaTestUtils.generateTestTableSchema(testTable);
    SourceRow sourceRow =
        SourceRow.builder(schema, null, 12412435345L)
            .setField("firstName", "abc")
            .setField("lastName", "def")
            .build();
    PCollection<SourceRow> sourceRows = pipeline.apply(Create.of(sourceRow));
    ISchemaMapper mockIschemaMapper =
        mock(ISchemaMapper.class, Mockito.withSettings().serializable());
    when(mockIschemaMapper.getDialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
    when(mockIschemaMapper.getSpannerTableName(anyString(), anyString()))
        .thenReturn("spannerTable");
    when(mockIschemaMapper.getSpannerColumnName(anyString(), anyString(), eq("firstName")))
        .thenReturn("spFirstName");
    when(mockIschemaMapper.getSpannerColumnName(anyString(), anyString(), eq("lastName")))
        .thenReturn("spLastName");
    when(mockIschemaMapper.getSourceColumnName(anyString(), anyString(), eq("spFirstName")))
        .thenReturn("firstName");
    when(mockIschemaMapper.getSourceColumnName(anyString(), anyString(), eq("spLastName")))
        .thenReturn("lastName");
    when(mockIschemaMapper.getSpannerColumnType(anyString(), anyString(), anyString()))
        .thenReturn(Type.string());
    when(mockIschemaMapper.getSpannerColumns(anyString(), anyString()))
        .thenReturn(List.of("spFirstName", "spLastName"));

    PCollection<Mutation> mutations =
        transform(sourceRows, SourceRowToMutationDoFn.create(mockIschemaMapper));

    PAssert.that(mutations)
        .containsInAnyOrder(
            Mutation.newInsertOrUpdateBuilder("spannerTable")
                .set("spFirstName")
                .to("abc")
                .set("spLastName")
                .to("def")
                .build());
    pipeline.run();
  }

  @Test
  public void testSourceRowToMutationDoFn_invalidTableUUID() {
    final String testTable = "srcTable";
    var schema = SchemaTestUtils.generateTestTableSchema(testTable);
    SourceRow sourceRow =
        SourceRow.builder(schema, null, 12412435345L)
            .setField("firstName", "abc")
            .setField("lastName", "def")
            .build();
    PCollection<SourceRow> sourceRows = pipeline.apply(Create.of(sourceRow));
    ISchemaMapper mockIschemaMapper =
        mock(ISchemaMapper.class, Mockito.withSettings().serializable());
    PCollection<Mutation> mutations =
        transform(sourceRows, SourceRowToMutationDoFn.create(mockIschemaMapper));

    PAssert.that(mutations).empty();
    pipeline.run();
  }

  @Test
  public void testSourceRowToMutationDoFn_transformException() {
    final String testTable = "srcTable";
    var schema = SchemaTestUtils.generateTestTableSchema(testTable);
    SourceRow sourceRow =
        SourceRow.builder(schema, null, 12412435345L)
            .setField("firstName", "abc")
            .setField("lastName", "def")
            .build();
    PCollection<SourceRow> sourceRows = pipeline.apply(Create.of(sourceRow));
    ISchemaMapper mockIschemaMapper =
        mock(ISchemaMapper.class, Mockito.withSettings().serializable());
    when(mockIschemaMapper.getSpannerTableName(anyString(), anyString()))
        .thenThrow(NoSuchElementException.class);

    PCollection<Mutation> mutations =
        transform(sourceRows, SourceRowToMutationDoFn.create(mockIschemaMapper));

    PAssert.that(mutations).empty();
    pipeline.run();
  }

  /** Helper Method to extract mutations from transformation output. */
  private PCollection<Mutation> transform(
      PCollection<SourceRow> sourceRows, SourceRowToMutationDoFn transformDoFn) {
    PCollectionTuple transform =
        sourceRows.apply(
            "Transform",
            ParDo.of(transformDoFn)
                .withOutputTags(
                    SourceDbToSpannerConstants.ROW_TRANSFORMATION_SUCCESS,
                    TupleTagList.of(SourceDbToSpannerConstants.ROW_TRANSFORMATION_ERROR)));
    transform
        .get(SourceDbToSpannerConstants.ROW_TRANSFORMATION_ERROR)
        .setCoder(SerializableCoder.of(RowContext.class)); // Need to set to run the pipeline
    PCollection<RowContext> successTransformation =
        transform
            .get(SourceDbToSpannerConstants.ROW_TRANSFORMATION_SUCCESS)
            .setCoder(SerializableCoder.of(RowContext.class));

    PCollection<Mutation> mutations =
        successTransformation.apply(
            MapElements.into(TypeDescriptor.of(Mutation.class))
                .via((RowContext r) -> r.mutation()));
    return mutations;
  }
}
