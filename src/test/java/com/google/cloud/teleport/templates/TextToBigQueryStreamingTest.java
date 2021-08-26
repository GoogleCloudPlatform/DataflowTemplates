/*
 * Copyright (C) 2020 Google LLC
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
package com.google.cloud.teleport.templates;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.values.FailsafeElement;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.Gson;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link TextToBigQueryStreaming}. */
@RunWith(JUnit4.class)
public class TextToBigQueryStreamingTest {

  private static final Gson GSON = new Gson();
  private static final String ERROR_MESSAGE = "test-error-message";
  private static final String NAME_KEY = "name";
  private static final String NAME_VALUE = "Jane";
  private static final String AGE_KEY = "age";
  private static final int AGE_VALUE = 20;
  private static TestPerson testPerson;

  @Before
  public void setup() {
    testPerson = new TestPerson();
    testPerson.age = AGE_VALUE;
    testPerson.name = NAME_VALUE;
  }

  @Test
  public void wrapBigQueryInsertErrorReturnsValidJSON() {
    TableRow testRow = new TableRow().set(NAME_KEY, testPerson.name).set(AGE_KEY, testPerson.age);
    InsertErrors insertErrors = new TableDataInsertAllResponse.InsertErrors();
    ErrorProto errorProto = new ErrorProto().setMessage(ERROR_MESSAGE);
    insertErrors.setErrors(ImmutableList.of(errorProto));
    TableReference tableReference = new TableReference();
    BigQueryInsertError bigQueryInsertError =
        new BigQueryInsertError(testRow.clone(), insertErrors, tableReference);
    String expected = GSON.toJson(testPerson);

    FailsafeElement<String, String> wrappedValue =
        TextToBigQueryStreaming.wrapBigQueryInsertError(bigQueryInsertError);
    String actualOriginalPayload = wrappedValue.getOriginalPayload();
    String actualPayload = wrappedValue.getPayload();
    String actualErrorMessage = wrappedValue.getErrorMessage();

    assertThat(actualOriginalPayload).isEqualTo(expected);
    assertThat(actualPayload).isEqualTo(expected);
    assertThat(actualErrorMessage).isEqualTo(GSON.toJson(insertErrors));
  }

  private static class TestPerson {

    private String name;
    private int age;
  }
}
