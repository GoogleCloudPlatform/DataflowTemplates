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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.BigQueryUtils;
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link BigQueryDeadLetterQueueSanitizerTest}. */
@RunWith(JUnit4.class)
public final class BigQueryDeadLetterQueueSanitizerTest {

  // Test the case where we can get json message and error json message from a failed BigQuery
  // insert.
  @Test
  public void testBigQueryDeadLetterQueueSanitizer() {
    TableRow tableRow = new TableRow();
    ObjectNode jsonNode = new ObjectNode(JsonNodeFactory.instance);
    jsonNode.put("SingerId", 1);
    jsonNode.put("FirstName", "Jack");
    tableRow.set(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_ORIGINAL_PAYLOAD_JSON, jsonNode.toString());
    tableRow.set("SingerId", 1);
    tableRow.set("FirstName", "Jack");
    InsertErrors insertErrors = new InsertErrors();
    ErrorProto errorProto = new ErrorProto();
    errorProto.setMessage("Some error message");
    errorProto.setDebugInfo("Some debug info");
    insertErrors.setErrors(Collections.singletonList(errorProto));
    TableReference tableReference = new TableReference();
    tableReference.set(
        BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_ORIGINAL_PAYLOAD_JSON,
        StandardSQLTypeName.STRING.name());
    tableReference.set("SingerId", StandardSQLTypeName.INT64.name());
    tableReference.set("FirstName", StandardSQLTypeName.STRING.name());
    BigQueryInsertError bigQueryInsertError =
        new BigQueryInsertError(tableRow, insertErrors, tableReference);

    assertThat(new BigQueryDeadLetterQueueSanitizer().getJsonMessage(bigQueryInsertError))
        .isEqualTo("{\"SingerId\":1,\"FirstName\":\"Jack\"}");
    assertThat(new BigQueryDeadLetterQueueSanitizer().getErrorMessageJson(bigQueryInsertError))
        .isEqualTo(
            "GenericData{classInfo=[errors, index], {errors=[GenericData{classInfo=[debugInfo,"
                + " location, message, reason], {debugInfo=Some debug info, message=Some error"
                + " message}}]}}");
  }
}
