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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.dofn.FetchSchemaFn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.sink.SinkSchemaFetcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SchemaLoaderTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  /** * A Fake Fetcher that returns a hardcoded schema instead of connecting to a DB. */
  private static class FakeMySqlSchemaFetcher implements SinkSchemaFetcher {
    @Override
    public void init(String path) {}

    @Override
    public DataGeneratorSchema getSchema() {
      return DataGeneratorSchema.builder()
          .tables(
              ImmutableMap.of(
                  "users",
                  DataGeneratorTable.builder()
                      .name("users")
                      .isRoot(true)
                      .columns(ImmutableList.of())
                      .primaryKeys(ImmutableList.of())
                      .foreignKeys(ImmutableList.of())
                      .uniqueKeys(ImmutableList.of())
                      .updateQps(0)
                      .deleteQps(0)
                      .recordsPerTick(1)
                      .build()))
          .build();
    }
  }

  /** * A Mock DoFn that overrides the fetcher creation logic. */
  private static class MockFetchSchemaFn extends FetchSchemaFn {
    public MockFetchSchemaFn(SinkType type, String path) {
      super(type, path);
    }

    @Override
    protected SinkSchemaFetcher createFetcher(SinkType sinkType) {
      return new FakeMySqlSchemaFetcher();
    }
  }

  @Test
  public void testSchemaLoader_verifiesTableKeys() throws IOException {

    File tempFile = tempFolder.newFile("shard.json");
    String dummyShardJson =
        "[{"
            + "\"host\":\"localhost\","
            + "\"port\":\"3306\","
            + "\"user\":\"root\","
            + "\"password\":\"password\","
            + "\"dbName\":\"test_db\""
            + "}]";

    Files.writeString(tempFile.toPath(), dummyShardJson, StandardCharsets.UTF_8);

    String validPath = tempFile.getAbsolutePath();

    MockFetchSchemaFn mockFn = new MockFetchSchemaFn(SinkType.MYSQL, validPath);
    SchemaLoader loader = new SchemaLoader(SinkType.MYSQL, validPath, mockFn);

    // Act
    PCollectionView<DataGeneratorSchema> schemaView = pipeline.apply(loader);

    // Assert
    PCollection<DataGeneratorSchema> collection =
        (PCollection<DataGeneratorSchema>) schemaView.getPCollection();

    PAssert.thatSingleton(collection)
        .satisfies(
            schema -> {
              assertNotNull(schema);
              assertTrue(schema.tables().containsKey("users"));
              return null;
            });

    pipeline.run().waitUntilFinish();
  }
}
