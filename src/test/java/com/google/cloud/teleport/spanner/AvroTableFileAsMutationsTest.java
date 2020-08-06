/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.spanner;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for {@link AvroTableFileAsMutations}. */
public class AvroTableFileAsMutationsTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testFileSharding() throws Exception {
    Path path = tmpFolder.newFile("testfile").toPath();
    int splitSize = 10000;
    Files.write(path, new byte[splitSize * 2]);
    MatchResult.Metadata fileMetadata =
        MatchResult.Metadata.builder()
            .setResourceId(FileSystems.matchNewResource(path.toString(), false /* isDirectory */))
            .setIsReadSeekEfficient(true)
            .setSizeBytes(splitSize * 2)
            .build();

    PAssert.that(runFileShardingPipeline(fileMetadata, splitSize))
        .satisfies(
            input -> {
              LinkedList<FileShard> shards = Lists.newLinkedList(input);
              assertThat(shards, hasSize(2));
              shards.forEach(
                  shard -> {
                    assertThat(
                        shard.getFile().getMetadata().resourceId().getFilename(),
                        equalTo("testfile"));
                    assertThat(shard.getTableName(), equalTo("testtable"));
                    assertThat(
                        shard.getRange().getTo() - shard.getRange().getFrom(),
                        equalTo(splitSize * 1L));
                  });
              return null;
            });
    p.run();
  }

  @Test
  public void testFileShardingNotSeekable() throws Exception {
    Path path = tmpFolder.newFile("testfile").toPath();
    int splitSize = 10000;
    Files.write(path, new byte[splitSize * 2]);
    MatchResult.Metadata fileMetadata =
        MatchResult.Metadata.builder()
            .setResourceId(FileSystems.matchNewResource(path.toString(), false /* isDirectory */))
            .setIsReadSeekEfficient(false)
            .setSizeBytes(splitSize * 2)
            .build();

    PAssert.that(runFileShardingPipeline(fileMetadata, splitSize))
        .satisfies(
            input -> {
              LinkedList<FileShard> shards = Lists.newLinkedList(input);
              assertThat(shards, hasSize(1));
              FileShard shard = shards.getFirst();
              assertThat(
                  shard.getFile().getMetadata().resourceId().getFilename(), equalTo("testfile"));
              assertThat(shard.getTableName(), equalTo("testtable"));
              assertThat(shard.getRange().getFrom(), equalTo(0L));
              assertThat(shard.getRange().getTo(), equalTo(splitSize * 2L));
              return null;
            });
    p.run();
  }

  @Test
  public void testFileShardingNoSharding() throws Exception {
    Path path = tmpFolder.newFile("testfile").toPath();
    int splitSize = 10000;
    Files.write(path, new byte[splitSize]);
    MatchResult.Metadata fileMetadata =
        MatchResult.Metadata.builder()
            .setResourceId(FileSystems.matchNewResource(path.toString(), false /* isDirectory */))
            .setIsReadSeekEfficient(true)
            .setSizeBytes(splitSize)
            .build();

    PAssert.that(runFileShardingPipeline(fileMetadata, splitSize))
        .satisfies(
            input -> {
              LinkedList<FileShard> shards = Lists.newLinkedList(input);
              assertThat(shards, hasSize(1));
              FileShard shard = shards.getFirst();
              assertThat(
                  shard.getFile().getMetadata().resourceId().getFilename(), equalTo("testfile"));
              assertThat(shard.getTableName(), equalTo("testtable"));
              assertThat(shard.getRange().getFrom(), equalTo(0L));
              assertThat(shard.getRange().getTo(), equalTo(splitSize * 1L));
              return null;
            });
    p.run();
  }

  private PCollection<FileShard> runFileShardingPipeline(Metadata fileMetadata, int splitSize) {

    PCollectionView<Map<String, String>> filenamesToTableNamesMapView =
        p.apply(
                "Create File/Table names Map",
                Create.of(
                    ImmutableMap.<String, String>of(
                        fileMetadata.resourceId().toString(), "testtable")))
            .apply(View.asMap());

    return p.apply("Create Metadata", Create.of(fileMetadata))
        .apply(FileIO.readMatches())
        // Pcollection<FileIO.ReadableFile>
        .apply(
            "Split into ranges",
            ParDo.of(new SplitIntoRangesFn(splitSize, filenamesToTableNamesMapView))
                .withSideInputs(filenamesToTableNamesMapView))
        .setCoder(FileShard.Coder.of());
  }

  @Test
  public void testAvroToMutationsTransform() throws Exception {
    DdlToAvroSchemaConverter converter = new DdlToAvroSchemaConverter("spannertest", "booleans");
    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("first_name")
            .string()
            .size(10)
            .endColumn()
            .column("last_name")
            .type(Type.string())
            .max()
            .endColumn()
            .column("full_name")
            .type(Type.string())
            .max()
            .generatedAs("CONCAT(first_name, ' ', last_name)")
            .stored()
            .endColumn()
            .primaryKey()
            .asc("id")
            .desc("last_name")
            .end()
            .endTable()
            .build();

    Collection<Schema> result = converter.convert(ddl);
    assertThat(result, hasSize(1));
    Schema usersSchema = result.iterator().next();

    GenericRecord user1 = new GenericData.Record(usersSchema);
    user1.put("id", 123L);
    user1.put("first_name", "John");
    user1.put("last_name", "Smith");
    user1.put("full_name", "John Smith");
    GenericRecord user2 = new GenericData.Record(usersSchema);
    user2.put("id", 456L);
    user2.put("first_name", "Jane");
    user2.put("last_name", "Doe");
    user2.put("full_name", "Jane Doe");

    File file = tmpFolder.newFile("users.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(usersSchema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(usersSchema, file);
    dataFileWriter.append(user1);
    dataFileWriter.append(user2);
    dataFileWriter.close();

    PCollectionView<Ddl> ddlView = p.apply("ddl", Create.of(ddl)).apply(View.asSingleton());

    PCollection<Mutation> mutations =
        p.apply("files/tables", Create.of(ImmutableMap.of(file.toPath().toString(), "Users")))
            .apply(new AvroTableFileAsMutations(ddlView));

    PAssert.that(mutations)
        .containsInAnyOrder(
            Mutation.newInsertOrUpdateBuilder("Users")
                .set("id")
                .to(123L)
                .set("first_name")
                .to("John")
                .set("last_name")
                .to("Smith")
                .build(),
            Mutation.newInsertOrUpdateBuilder("Users")
                .set("id")
                .to(456L)
                .set("first_name")
                .to("Jane")
                .set("last_name")
                .to("Doe")
                .build());
    p.run();
  }
}
