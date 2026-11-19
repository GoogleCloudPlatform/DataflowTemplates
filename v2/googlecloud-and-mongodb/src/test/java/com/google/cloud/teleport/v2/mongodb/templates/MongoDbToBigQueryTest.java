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
package com.google.cloud.teleport.v2.mongodb.templates;

import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.v2.mongodb.templates.MongoDbToBigQuery.Options;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link MongoDbToBigQuery}. */
@RunWith(JUnit4.class)
public class MongoDbToBigQueryTest {

  private static final String MONGO_DB_URI = "mongodb://localhost:27017";

  @Test
  public void testBuildReadDocumentsDefaultsToSingleSource() {
    Options options =
        PipelineOptionsFactory.fromArgs("--database=my-db", "--collection=my-collection")
            .as(Options.class);

    MongoDbIO.Read read = MongoDbToBigQuery.buildReadDocuments(options, MONGO_DB_URI);

    assertEquals("false", displayValue(read, "bucketAuto"));
    assertEquals("0", displayValue(read, "numSplit"));
  }

  @Test
  public void testBuildReadDocumentsWithBucketAutoAndNumSplits() {
    Options options =
        PipelineOptionsFactory.fromArgs(
                "--database=my-db",
                "--collection=my-collection",
                "--bucketAuto=true",
                "--numSplits=32")
            .as(Options.class);

    MongoDbIO.Read read = MongoDbToBigQuery.buildReadDocuments(options, MONGO_DB_URI);

    assertEquals("true", displayValue(read, "bucketAuto"));
    assertEquals("32", displayValue(read, "numSplit"));
  }

  private static String displayValue(MongoDbIO.Read read, String key) {
    return DisplayData.from(read).items().stream()
        .filter(item -> item.getKey().equals(key))
        .map(item -> String.valueOf(item.getValue()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Missing display data: " + key));
  }
}
