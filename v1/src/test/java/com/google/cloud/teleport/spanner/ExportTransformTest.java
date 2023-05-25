/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.spanner;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.spanner.ExportTransform.BuildTableManifests;
import com.google.cloud.teleport.spanner.ExportTransform.CombineTableMetadata;
import com.google.cloud.teleport.spanner.ExportTransform.CreateDatabaseManifest;
import com.google.cloud.teleport.spanner.ExportTransform.SchemaBasedDynamicDestinations;
import com.google.cloud.teleport.spanner.ExportTransform.SerializableSchemaString;
import com.google.cloud.teleport.spanner.ExportTransform.SerializableSchemaSupplier;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.proto.ExportProtos.Export;
import com.google.cloud.teleport.spanner.proto.ExportProtos.Export.Builder;
import com.google.cloud.teleport.spanner.proto.ExportProtos.Export.Table;
import com.google.cloud.teleport.spanner.proto.ExportProtos.ProtoDialect;
import com.google.cloud.teleport.spanner.proto.ExportProtos.TableManifest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for ExportTransform. */
public class ExportTransformTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void buildTableManifests() throws Exception {
    Path f1 = Files.createTempFile("table1-file", "1");
    Path f2 = Files.createTempFile("table1-file", "2");
    Path f3 = Files.createTempFile("table2-file", "1");

    // Create the expected manifest string.
    TableManifest.Builder builder = TableManifest.newBuilder();
    builder
        .addFilesBuilder()
        .setName(f1.getFileName().toString())
        .setMd5("1B2M2Y8AsgTpgAmY7PhCfg==");
    builder
        .addFilesBuilder()
        .setName(f2.getFileName().toString())
        .setMd5("1B2M2Y8AsgTpgAmY7PhCfg==");
    String manifest1 = JsonFormat.printer().print(builder.build());

    builder = TableManifest.newBuilder();
    builder
        .addFilesBuilder()
        .setName(f3.getFileName().toString())
        .setMd5("1B2M2Y8AsgTpgAmY7PhCfg==");
    String manifest2 = JsonFormat.printer().print(builder.build());

    final Map<String, Iterable<String>> tablesAndFiles =
        ImmutableMap.of(
            "table1", ImmutableList.of(f1.toString(), f2.toString()),
            "table2", ImmutableList.of(f3.toString()));

    // Execute the transform.
    PCollection<KV<String, String>> tableManifests =
        pipeline
            .apply("Create", Create.of(tablesAndFiles))
            .apply("Build table manifest", ParDo.of(new BuildTableManifests()));

    PAssert.that(tableManifests)
        .containsInAnyOrder(KV.of("table1", manifest1), KV.of("table2", manifest2));
    pipeline.run();
  }

  @Test
  public void buildEmptyTableManifests() throws Exception {
    final Map<String, Iterable<String>> tablesAndFiles =
        ImmutableMap.of("empty-cloud-spanner-export", ImmutableList.of("file1"));

    // Execute the transform.
    PCollection<KV<String, String>> tableManifests =
        pipeline
            .apply("Create", Create.of(tablesAndFiles))
            .apply("Build table manifest", ParDo.of(new BuildTableManifests()));
    PAssert.that(tableManifests).empty();
    pipeline.run();
  }

  @Test
  public void buildDatabaseManifestFile() throws InvalidProtocolBufferException {
    Map<String, String> tablesAndManifests =
        ImmutableMap.of(
            "model1",
            "model1 manifest",
            "table1",
            "table1 manifest",
            "table2",
            "table2 manifest",
            "changeStream",
            "changeStream manifest");

    PCollection<List<Export.Table>> metadataTables =
        pipeline
            .apply("Initialize table manifests", Create.of(tablesAndManifests))
            .apply("Combine table manifests", Combine.globally(new CombineTableMetadata()));

    ImmutableList<Export.DatabaseOption> databaseOptions =
        ImmutableList.of(
            Export.DatabaseOption.newBuilder()
                .setOptionName("version_retention_period")
                .setOptionValue("5d")
                .build());
    Ddl.Builder ddlBuilder = Ddl.builder();
    ddlBuilder.mergeDatabaseOptions(databaseOptions);
    ddlBuilder.createModel("model1").remote(true).endModel();
    ddlBuilder.createChangeStream("changeStream").endChangeStream();
    Ddl ddl = ddlBuilder.build();
    PCollectionView<Ddl> ddlView = pipeline.apply(Create.of(ddl)).apply(View.asSingleton());
    PCollectionView<Dialect> dialectView =
        pipeline
            .apply("CreateSingleton", Create.of(Dialect.GOOGLE_STANDARD_SQL))
            .apply("As PCollectionView", View.asSingleton());
    PCollection<String> databaseManifest =
        metadataTables.apply(
            "Test adding database option to manifest",
            ParDo.of(new CreateDatabaseManifest(ddlView, dialectView))
                .withSideInputs(ddlView, dialectView));

    // The output JSON may contain the tables in any order, so a string comparison is not
    // sufficient. Have to convert the manifest string to a protobuf. Also for the checker function
    // to be serializable, it has to be written as a lambda.
    PAssert.thatSingleton(databaseManifest)
        .satisfies( // Checker function.
            (SerializableFunction<String, Void>)
                input -> {
                  Builder builder1 = Export.newBuilder();
                  try {
                    JsonFormat.parser().merge(input, builder1);
                  } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                  }
                  Export manifestProto = builder1.build();
                  assertThat(manifestProto.getDialect(), is(ProtoDialect.GOOGLE_STANDARD_SQL));

                  assertThat(manifestProto.getTablesCount(), is(3));
                  for (Table table : manifestProto.getTablesList()) {
                    assertThat(table.getName(), anyOf(startsWith("table"), startsWith("model")));
                    assertThat(table.getManifestFile(), is(table.getName() + "-manifest.json"));
                  }

                  Export.DatabaseOption dbOptions = manifestProto.getDatabaseOptions(0);
                  String optionName = dbOptions.getOptionName();
                  String optionValue = dbOptions.getOptionValue();
                  assertThat(optionName, is("version_retention_period"));
                  assertThat(optionValue, is("5d"));

                  assertThat(manifestProto.getChangeStreamsCount(), is(1));
                  assertThat(manifestProto.getChangeStreams(0).getName(), is("changeStream"));
                  assertThat(
                      manifestProto.getChangeStreams(0).getManifestFile(),
                      is("changeStream-manifest.json"));
                  return null;
                });

    pipeline.run();
  }

  @Test
  public void createTimestampBound_noTimestamp() {
    assertEquals(TimestampBound.strong(), ExportTransform.createTimestampBound(""));
  }

  @Test
  public void createTimestampBound_zeroTimeZoneOffset() {
    assertEquals(
        TimestampBound.ofReadTimestamp(Timestamp.ofTimeSecondsAndNanos(946782245, 0)),
        ExportTransform.createTimestampBound("2000-01-02T03:04:05Z"));
  }

  @Test
  public void createTimestampBound_nonZeroTimeZoneOffset() {
    assertEquals(
        TimestampBound.ofReadTimestamp(Timestamp.ofTimeSecondsAndNanos(946803845, 0)),
        ExportTransform.createTimestampBound("2000-01-02T03:04:05-06:00"));
  }

  @Test
  public void createTimestampBound_noTimeZoneOffset() {
    assertEquals(
        TimestampBound.ofReadTimestamp(Timestamp.ofTimeSecondsAndNanos(946782245, 0)),
        ExportTransform.createTimestampBound("2000-01-02T03:04:05"));
  }

  @Test
  public void createTimestampBound_invalidTimestamp() {
    assertThrows(
        IllegalStateException.class,
        () -> ExportTransform.createTimestampBound("2000-01-02TT03:04:05"));
  }

  @Test
  public void createTimestampBound_futureTimestamp() {
    assertThrows(
        IllegalStateException.class,
        () ->
            ExportTransform.createTimestampBound(
                Timestamp.ofTimeMicroseconds(
                        Timestamp.now().getSeconds() * 1000000L + 360000000000L)
                    .toString()));
  }

  @Test
  public void testSerializableSchema() throws IOException, ClassNotFoundException {
    Schema schema =
        SchemaBuilder.builder().record("record").fields().requiredLong("id").endRecord();

    ExportTransform.SerializableSchemaString serializableSchemaString =
        new SerializableSchemaString(schema.toString());
    SerializableSchemaSupplier supplier =
        (SerializableSchemaSupplier) serializableSchemaString.readResolve();
    assertEquals(schema.toString(), supplier.get().toString());
    assertTrue(supplier.equals(supplier));
    assertFalse(supplier.equals(Boolean.TRUE));
    SerializableSchemaSupplier supplier2 =
        (SerializableSchemaSupplier) serializableSchemaString.readResolve();
    assertTrue(supplier.equals(supplier2));
    assertNotNull(supplier.writeReplace());
    assertNotNull(supplier.hashCode());
  }

  /** Tests for SchemaBasedDynamicDestinations class. */
  public static class SchemaBasedDynamicDestinationsTest {

    @Rule public final transient TestPipeline pipeline = TestPipeline.create();
    @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();

    @Test
    public void testGetDestination() {
      Struct struct = Struct.newBuilder().add(Value.string("string")).build();
      SchemaBasedDynamicDestinations schemaBasedDynamicDestinations =
          new SchemaBasedDynamicDestinations(null, null, null, null);
      assertEquals("string", schemaBasedDynamicDestinations.getDestination(struct));
    }

    @Test
    public void testSideInput() {
      SchemaBasedDynamicDestinations schemaBasedDynamicDestinations =
          new SchemaBasedDynamicDestinations(null, null, null, null);
      assertEquals(3, schemaBasedDynamicDestinations.getSideInputs().size());
    }
  }
}
