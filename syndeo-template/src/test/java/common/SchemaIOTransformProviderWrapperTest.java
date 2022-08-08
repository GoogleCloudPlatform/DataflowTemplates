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
package common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.auto.service.AutoService;
import com.google.cloud.syndeo.common.SchemaIOTransformProviderWrapper;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SchemaIOTransformProviderWrapperTest {
  @Rule public final transient TestPipeline p = TestPipeline.create();

  @AutoService(SchemaIOProvider.class)
  public static class FakeSchemaIOProvider implements SchemaIOProvider {

    public FakeSchemaIOProvider() {}

    @Override
    public String identifier() {
      return "fake:v1";
    }

    @Override
    public Schema configurationSchema() {
      return Schema.of(Field.of("number", FieldType.INT32));
    }

    @Override
    public SchemaIO from(String location, Row configuration, @Nullable Schema dataSchema) {
      return new FakeSchemaIO(location, configuration, dataSchema);
    }

    @Override
    public boolean requiresDataSchema() {
      return true;
    }

    @Override
    public IsBounded isBounded() {
      return IsBounded.BOUNDED;
    }
  }

  private static class FakeSchemaIO implements SchemaIO, Serializable {
    String location;
    int number;
    Schema schema;

    public FakeSchemaIO(String location, Row configuration, @Nullable Schema schema) {
      this.location = location;
      this.number = configuration.getInt32("number");
      this.schema = schema;
    }

    @Override
    public Schema schema() {
      return schema;
    }

    private Row getDefaultRow() {
      String result = location + number;
      return Row.withSchema(schema)
          .addValues(Collections.nCopies(schema.getFieldCount(), result))
          .build();
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> buildReader() {
      return new PTransform<PBegin, PCollection<Row>>() {
        @Override
        public PCollection<Row> expand(PBegin begin) {
          return begin.apply(
              Create.of(Collections.nCopies(number, getDefaultRow()))
                  .withCoder(RowCoder.of(schema))
                  .withRowSchema(schema));
        }
      };
    }

    @Override
    public PTransform<PCollection<Row>, ? extends POutput> buildWriter() {
      return new PTransform<PCollection<Row>, PDone>() {
        @Override
        public PDone expand(PCollection<Row> input) {
          PCollection<KV<Integer, Iterable<Row>>> grouped =
              input
                  .apply(WithKeys.of((Row r) -> 1))
                  .setCoder(KvCoder.of(VarIntCoder.of(), input.getCoder()))
                  .apply(GroupByKey.create());
          grouped.apply(
              ParDo.of(
                  new DoFn<KV<Integer, Iterable<Row>>, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      int count = 0;
                      for (Row row : c.element().getValue()) {
                        assertEquals(row, getDefaultRow());
                        count++;
                      }
                      assertEquals(count, number);
                    }
                  }));
          return PDone.in(input.getPipeline());
        }
      };
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSchemaWrapperRead() throws Exception {
    FakeSchemaIOProvider fakeProvider = new FakeSchemaIOProvider();
    SchemaIOTransformProviderWrapper wrapper =
        new SchemaIOTransformProviderWrapper(fakeProvider, true);

    Schema schema = Schema.of(Field.of("a", FieldType.STRING), Field.of("b", FieldType.STRING));
    Row config = Row.withSchema(wrapper.configurationSchema()).addValues("loc", schema, 3).build();

    assertTrue(wrapper.outputCollectionNames().size() == 1);
    PCollection<Row> read =
        PCollectionRowTuple.empty(p)
            .apply(wrapper.from(config).buildTransform())
            .get(wrapper.outputCollectionNames().get(0));
    Row expected = Row.withSchema(schema).addValues("loc3", "loc3").build();
    PAssert.that(read).containsInAnyOrder(expected, expected, expected);
    p.run().waitUntilFinish();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSchemaWrapperWrite() throws Exception {
    FakeSchemaIOProvider fakeProvider = new FakeSchemaIOProvider();
    SchemaIOTransformProviderWrapper wrapper =
        new SchemaIOTransformProviderWrapper(fakeProvider, false);

    Schema schema = Schema.of(Field.of("a", FieldType.STRING), Field.of("b", FieldType.STRING));
    Row config = Row.withSchema(wrapper.configurationSchema()).addValues("loc", schema, 3).build();

    assertTrue(wrapper.inputCollectionNames().size() == 1);
    Row inputRow = Row.withSchema(schema).addValues("loc3", "loc3").build();
    PCollection<Row> toWrite =
        p.apply(
            Create.of(inputRow, inputRow, inputRow)
                .withCoder(RowCoder.of(schema))
                .withRowSchema(schema));

    PCollectionRowTuple.of(wrapper.inputCollectionNames().get(0), toWrite)
        .apply(wrapper.from(config).buildTransform());
    p.run().waitUntilFinish();
  }

  @Test
  public void testGetAll() throws Exception {
    List<SchemaTransformProvider> providers = SchemaIOTransformProviderWrapper.getAll();
    assertEquals(
        1,
        providers.stream()
            .filter((provider) -> provider.identifier().equals("schemaIO:fake:v1:read"))
            .count());
    assertEquals(
        1,
        providers.stream()
            .filter((provider) -> provider.identifier().equals("schemaIO:fake:v1:write"))
            .count());
  }
}
