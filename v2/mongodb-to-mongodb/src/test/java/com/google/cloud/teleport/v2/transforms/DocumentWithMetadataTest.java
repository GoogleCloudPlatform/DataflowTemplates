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
package com.google.cloud.teleport.v2.transforms;

import static com.google.cloud.teleport.v2.transforms.DocumentWithMetadata.ErrorType.RETRYABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DocumentWithMetadataTest {

  @Test
  public void documentWithMetadata_toAndFromDlqJson_roundTrip() {
    Document doc = new Document("_id", 1).append("name", "test");
    String originalDocStr = doc.toJson();
    DocumentWithMetadata original =
        DocumentWithMetadata.of(
            doc, originalDocStr, 1, "Error message", RETRYABLE, "srcCol", "tgtCol");

    String dlqJson = original.toDlqJson("Error message", RETRYABLE, 1);
    DocumentWithMetadata reconstructed = DocumentWithMetadata.fromDlqJson(dlqJson);

    assertEquals(original.getDocument(), reconstructed.getDocument());
    assertEquals(
        Document.parse(original.getOriginalDocument()),
        Document.parse(reconstructed.getOriginalDocument()));
    assertEquals(original.getRetryCount(), reconstructed.getRetryCount());
    assertEquals(original.getErrorMessage(), reconstructed.getErrorMessage());
    assertEquals(original.getErrorType(), reconstructed.getErrorType());
    assertEquals(original.getSourceCollection(), reconstructed.getSourceCollection());
    assertEquals(original.getTargetCollection(), reconstructed.getTargetCollection());
  }

  @Test(expected = RuntimeException.class)
  public void documentWithMetadata_fromDlqJson_invalidJson_throwsException() {
    DocumentWithMetadata.fromDlqJson("invalid json");
  }

  @Test(expected = RuntimeException.class)
  public void documentWithMetadata_fromDlqJson_missingData_throwsException() {
    DocumentWithMetadata.fromDlqJson("{\"message\": {}}");
  }

  @Test
  public void documentWithMetadata_of_storesCanonicalJson() {
    Document doc =
        new Document()
            .append("doubleVal", 1.23)
            .append("longVal", 123L)
            .append("nanVal", Double.NaN)
            .append("infVal", Double.POSITIVE_INFINITY)
            .append("negInfVal", Double.NEGATIVE_INFINITY);

    DocumentWithMetadata item = DocumentWithMetadata.of(doc);
    String original = item.getOriginalDocument();

    assertTrue(
        "Should contain canonical double",
        original.contains("\"doubleVal\": {\"$numberDouble\": \"1.23\"}"));
    assertTrue(
        "Should contain canonical long",
        original.contains("\"longVal\": {\"$numberLong\": \"123\"}"));
    assertTrue(
        "Should contain canonical NaN",
        original.contains("\"nanVal\": {\"$numberDouble\": \"NaN\"}"));
    assertTrue(
        "Should contain canonical Infinity",
        original.contains("\"infVal\": {\"$numberDouble\": \"Infinity\"}"));
    assertTrue(
        "Should contain canonical negative Infinity",
        original.contains("\"negInfVal\": {\"$numberDouble\": \"-Infinity\"}"));
  }

  @Test
  public void documentWithMetadata_getId_returnsId() {
    Document doc = new Document("_id", 1).append("name", "test");
    DocumentWithMetadata item = DocumentWithMetadata.of(doc);
    assertEquals(1, item.getId());
  }

  @Test
  public void documentWithMetadata_getId_returnsNullWhenNoId() {
    Document doc = new Document("name", "test");
    DocumentWithMetadata item = DocumentWithMetadata.of(doc);
    assertEquals(null, item.getId());
  }
}
