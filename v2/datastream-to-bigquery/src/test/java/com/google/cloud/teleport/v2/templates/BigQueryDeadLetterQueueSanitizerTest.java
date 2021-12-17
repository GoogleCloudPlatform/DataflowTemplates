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
package com.google.cloud.teleport.v2.templates;

import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.FailsafeJavascriptUdf;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertErrorCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BigQueryDeadLetterQueueSanitizer}. */
@RunWith(JUnit4.class)
public class BigQueryDeadLetterQueueSanitizerTest implements Serializable {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Tests the {@link FailsafeJavascriptUdf} when the input is valid. */
  @Test
  @Category(NeedsRunner.class)
  public void testRunJsonError() {
    String jsonMessage = "{\"key\":\"valué\"}";
    TableRow tableRow = BigQueryConverters.convertJsonToTableRow(jsonMessage);
    BigQueryInsertError errorMessage = getBigQueryInsertError(tableRow, "something happened");
    List<String> expectedJson =
        Arrays.asList(
            "{\"message\":{\"key\":\"valué\"},\"error_message\":\"GenericData{classInfo=[errors,"
                + " index], {errors=[GenericData{classInfo=[debugInfo, location, message, reason],"
                + " {message=something happened}}]}}\"}");

    PCollection<String> output =
        pipeline
            .apply("CreateInput", Create.of(errorMessage).withCoder(BigQueryInsertErrorCoder.of()))
            .apply("BigQuery Failures", MapElements.via(new BigQueryDeadLetterQueueSanitizer()));

    PAssert.that(output).containsInAnyOrder(expectedJson);

    // Execute the test
    pipeline.run();
  }

  /**
   * Generates a {@link BigQueryInsertError} with the {@link GenericRecord} and error message.
   *
   * @param record payload to be used for the test
   * @param errorMessage error message for the test
   */
  private static BigQueryInsertError getBigQueryInsertError(
      TableRow tableRow, String errorMessage) {
    TableReference tableReference = new TableReference();

    return new BigQueryInsertError(tableRow.clone(), getInsertErrors(errorMessage), tableReference);
  }

  /**
   * Generates a {@link InsertErrors} used by {@link BigQueryInsertError}.
   *
   * @param error string to be added to {@link BigQueryInsertError}
   */
  private static InsertErrors getInsertErrors(String error) {
    InsertErrors insertErrors = new TableDataInsertAllResponse.InsertErrors();
    ErrorProto errorProto = new ErrorProto().setMessage(error);
    insertErrors.setErrors(Lists.newArrayList(errorProto));

    return insertErrors;
  }
}
