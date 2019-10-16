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
package com.google.cloud.teleport.v2.templates;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test cases for {@link BigQueryToElasticsearch}. */
public class BigQueryToElasticsearchTest {

  private static final TableRow tableRow =
      new TableRow().set("id", "007").set("state", "CA").set("price", 26.23);
  private static final List<TableRow> rows = ImmutableList.of(tableRow);
  private static final String jsonifiedTableRow =
      "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  /** Test the {@link BigQueryToElasticsearch} pipeline end-to-end. */
  @Test
  public void testBigQueryToElasticsearchE2E() {

    BigQueryToElasticsearch.BigQueryToElasticsearchReadOptions options =
        PipelineOptionsFactory.create()
            .as(BigQueryToElasticsearch.BigQueryToElasticsearchReadOptions.class);

    options.setNodeAddresses("http://my-node");
    options.setIndex("test");
    options.setDocumentType("_doc");
    options.setInputTableSpec("my-project:my-dataset.my-table");
    options.setQuery(null);

    // Build pipeline
    PCollection<String> testStrings =
        pipeline
            .apply("CreateInput", Create.of(rows))
            .apply("TestTableRowToJson", ParDo.of(new BigQueryConverters.TableRowToJsonFn()));

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

  /**
   * Tests that the {@link BigQueryToElasticsearch} pipeline throws an {@link
   * IllegalArgumentException} when no query or input table spec is provided.
   */
  @Test
  public void testNoQueryOrInputTableSpec() {
    exceptionRule.expect(IllegalArgumentException.class);

    BigQueryToElasticsearch.BigQueryToElasticsearchReadOptions options =
        PipelineOptionsFactory.create()
            .as(BigQueryToElasticsearch.BigQueryToElasticsearchReadOptions.class);

    options.setNodeAddresses("http://my-node");
    options.setIndex("test");
    options.setDocumentType("_doc");
    options.setInputTableSpec(null);
    options.setQuery(null);

    // Build pipeline
    pipeline.apply("CreateInput", Create.of(tableRow));

    // Execute pipeline
    pipeline.run();
  }
}
