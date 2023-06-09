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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.ImportTransform.ReadAvroSchemas;
import com.google.cloud.teleport.spanner.ImportTransform.ReadExportManifestFile;
import com.google.cloud.teleport.spanner.ImportTransform.ReadManifestFiles;
import com.google.cloud.teleport.spanner.ImportTransform.ReadTableManifestFile;
import com.google.cloud.teleport.spanner.ImportTransform.ValidateInputFiles;
import com.google.cloud.teleport.spanner.proto.ExportProtos.Export;
import com.google.cloud.teleport.spanner.proto.ExportProtos.TableManifest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for ImportTransform class. */
public class ImportTransformTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void validateInputFiles() throws Exception {
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
    TableManifest manifest1 = builder.build();

    builder = TableManifest.newBuilder();
    builder
        .addFilesBuilder()
        .setName(f3.getFileName().toString())
        .setMd5("1B2M2Y8AsgTpgAmY7PhCfg==");
    TableManifest manifest2 = builder.build();

    final Map<String, TableManifest> tablesAndManifests =
        ImmutableMap.of(
            "table1", manifest1,
            "table2", manifest2);
    ValueProvider<String> importDirectory =
        ValueProvider.StaticValueProvider.of(f1.getParent().toString());

    // Execute the transform.
    PCollection<KV<String, String>> tableAndFiles =
        pipeline
            .apply("Create", Create.of(tablesAndManifests))
            .apply(ParDo.of(new ValidateInputFiles(importDirectory)));

    PAssert.that(tableAndFiles)
        .containsInAnyOrder(
            KV.of("table1", f1.toString()),
            KV.of("table1", f2.toString()),
            KV.of("table2", f3.toString()));

    pipeline.run();
  }

  @Test(expected = PipelineExecutionException.class)
  public void validateInvalidInputFiles() throws Exception {
    Path f1 = Files.createTempFile("table1-file", "1");
    TableManifest.Builder builder = TableManifest.newBuilder();
    builder.addFilesBuilder().setName(f1.getFileName().toString()).setMd5("invalid checksum");
    TableManifest manifest1 = builder.build();
    ValueProvider<String> importDirectory =
        ValueProvider.StaticValueProvider.of(f1.getParent().toString());
    // Execute the transform.
    pipeline
        .apply("Create", Create.of(ImmutableMap.of("table1", manifest1)))
        .apply(ParDo.of(new ValidateInputFiles(importDirectory)));
    pipeline.run();
    // Pipeline should fail with an exception.
  }

  @Test
  public void testReadExportManifestFile() throws Exception {
    Path path = tmpFolder.newFile("spanner-export.json").toPath();

    String testManifest =
        "{\n"
            + "  \"tables\": [\n"
            + "    {\n"
            + "       \"name\": \"Person\",\n"
            + "       \"manifestFile\": \"Person-manifest.json\""
            + "    }\n"
            + "  ]\n"
            + "}";

    Files.write(path, testManifest.getBytes());

    PCollectionView<Dialect> dialectView =
        pipeline
            .apply("Dialect", Create.of(Dialect.GOOGLE_STANDARD_SQL))
            .apply("Dialect As PCollectionView", View.asSingleton());
    PCollection<Export> manifest =
        pipeline.apply(
            "Read manifest",
            new ReadExportManifestFile(
                ValueProvider.StaticValueProvider.of(tmpFolder.getRoot().getAbsolutePath()),
                dialectView));

    PAssert.that(manifest)
        .satisfies(
            input -> {
              LinkedList<Export> manifests = Lists.newLinkedList(input);
              assertEquals(1, manifests.size());
              for (Export export : manifests) {
                assertEquals(1, export.getTablesCount());
                assertEquals("Person", export.getTables(0).getName());
                assertEquals("Person-manifest.json", export.getTables(0).getManifestFile());
              }
              return null;
            });
    pipeline.run();
  }

  @Test
  public void testReadTableManifestFile() throws Exception {
    Path path = tmpFolder.newFile("Person-manifest.json").toPath();

    String testManifest =
        "{\n"
            + "  \"files\": [{\n"
            + "    \"name\": \"person.avro-00000-of-00005\",\n"
            + "    \"md5\": \"custom-md5\"\n"
            + "  }]"
            + "}";

    Files.write(path, testManifest.getBytes());

    PCollection<KV<String, String>> input =
        pipeline.apply(
            "Dialect",
            Create.of(
                new HashMap<String, String>() {
                  {
                    put("Person", "Person-manifest.json");
                  }
                }));
    PCollection<KV<String, TableManifest>> tableManifests =
        input.apply(
            "Read manifest",
            new ReadTableManifestFile(
                StaticValueProvider.of(tmpFolder.getRoot().getAbsolutePath())));

    PAssert.that(tableManifests)
        .satisfies(
            input1 -> {
              LinkedList<KV<String, TableManifest>> manifests = Lists.newLinkedList(input1);
              assertEquals(1, manifests.size());
              for (KV<String, TableManifest> manifest : manifests) {
                assertEquals("Person", manifest.getKey());
                assertEquals(1, manifest.getValue().getFilesCount());
                assertEquals(
                    "person.avro-00000-of-00005", manifest.getValue().getFiles(0).getName());
                assertEquals("custom-md5", manifest.getValue().getFiles(0).getMd5());
              }
              return null;
            });
    pipeline.run();
  }

  @Test
  public void testReadManifestFiles() throws Exception {
    String testManifest =
        "{\n"
            + "  \"tables\": [\n"
            + "    {\n"
            + "       \"name\": \"Person\",\n"
            + "       \"manifestFile\": \"Person-manifest.json\""
            + "    }\n"
            + "  ]\n"
            + "}";
    Path path = tmpFolder.newFile("spanner-export.json").toPath();
    Files.write(path, testManifest.getBytes());

    path = tmpFolder.newFile("person.avro-00000-of-00005").toPath();
    Files.write(path, new byte[20]);

    String testTableManifest =
        "{\n"
            + "  \"files\": [{\n"
            + "    \"name\": \"person.avro-00000-of-00005\",\n"
            + "    \"md5\": \""
            + FileChecksum.getLocalFileChecksum(path)
            + "\"\n"
            + "  }]"
            + "}";
    path = tmpFolder.newFile("Person-manifest.json").toPath();
    Files.write(path, testTableManifest.getBytes());

    PCollectionView<Dialect> dialectView =
        pipeline
            .apply("Dialect", Create.of(Dialect.GOOGLE_STANDARD_SQL))
            .apply("Dialect As PCollectionView", View.asSingleton());
    PCollection<Export> manifest =
        pipeline.apply(
            "Read manifest",
            new ReadExportManifestFile(
                ValueProvider.StaticValueProvider.of(tmpFolder.getRoot().getAbsolutePath()),
                dialectView));

    PCollection<KV<String, String>> allFiles =
        manifest.apply(
            "Read all manifest files",
            new ReadManifestFiles(
                ValueProvider.StaticValueProvider.of(tmpFolder.getRoot().getAbsolutePath())));

    PAssert.that(allFiles)
        .satisfies(
            input -> {
              LinkedList<KV<String, String>> manifests = Lists.newLinkedList(input);
              assertEquals(1, manifests.size());
              for (KV<String, String> file : manifests) {
                assertEquals("Person", file.getKey());
                assertTrue(file.getValue().contains("person.avro-00000-of-00005"));
              }
              return null;
            });
    pipeline.run();
  }

  @Test
  public void testReadAvroSchema() throws Exception {
    Schema schema =
        SchemaBuilder.builder().record("record").fields().requiredLong("id").endRecord();

    GenericRecord avroRecord = new GenericRecordBuilder(schema).set("id", 0L).build();

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    Path path = tmpFolder.newFile("person.avro-00000-of-00005").toPath();

    dataFileWriter.create(schema, Files.newOutputStream(path));

    PCollection<KV<String, String>> input =
        pipeline.apply(
            "Dialect",
            Create.of(
                new HashMap<String, String>() {
                  {
                    put("Person", path.toAbsolutePath().toString());
                  }
                }));

    PCollection<KV<String, String>> avroSchema =
        input.apply("Read avro schema", ParDo.of(new ReadAvroSchemas()));

    PAssert.that(avroSchema)
        .satisfies(
            input1 -> {
              Schema schema1 =
                  SchemaBuilder.builder().record("record").fields().requiredLong("id").endRecord();
              LinkedList<KV<String, String>> manifests = Lists.newLinkedList(input1);
              assertEquals(1, manifests.size());
              for (KV<String, String> file : manifests) {
                assertEquals("Person", file.getKey());
                assertEquals(schema1.toString(), file.getValue());
              }
              return null;
            });
    pipeline.run();
  }
}
