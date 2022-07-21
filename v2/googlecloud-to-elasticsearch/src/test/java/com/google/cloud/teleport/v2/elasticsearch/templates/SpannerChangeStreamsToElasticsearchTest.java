/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.elasticsearch.templates;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.Timestamp;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ValueCaptureType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test cases for {@link SpannerChangeStreamsToElasticsearch}. */
public class SpannerChangeStreamsToElasticsearchTest {
  private static final List<ColumnnType> rowType = ImmutableList.of(
    new ColumnType("UserId", new TypeCode("STRING"), true, 1),
    new ColumnType("AccountId", new TypeCode("STRING"), true, 2),
    new ColumnType("LastUpdate", new TypeCode("TIMESTAMP"), false, 3),
    new ColumnType("Balance", new TypeCode("INT"), false, 4)
  );
  private static final List<Mod> insertMods = ImmutableList.of(
    new Mod("", "", ""),
    new Mod("", "", "")
  );
  private static final DataChangeRecord insertRow =
    new DataChangeRecord("partitionToken1", Timestamp.now(), "transactionId1", true, "recordSequence1","testTable", rowType, insertMods, ModType.INSERT, ValueCaptureType.OLD_AND_NEW_VALUES, 1, 1,null);
  private static final List<DataChangeRecord> rows = ImmutableList.of(insertRow);
  private static final String jsonifiedInsertleRow =
    "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException exceptionRule = ExpectedException.none();

    /** Test the {@link SpannerChangeStreamsToElasticsearch} pipeline end-to-end. */
    @Test
    public void testSpannerChangeStreamsToElasticsearchE2E() {
  
      // Build pipeline
      PCollection<String> testStrings =
          pipeline
              .apply("CreateInput", Create.of(rows))
              .apply("DataChangeRecordToJson", ParDo.of(new DataChangeRecordToJsonFn()));
  
      PAssert.that(testStrings)
          .satisfies(
              collection -> {
                String result = collection.iterator().next();
                assertThat(result, is(equalTo(jsonifiedTableRow)));
                return null;
              });
  
      // Execute pipeline
      pipeline.run();
    }
}
