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
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DocumentWithMetadataTest {

  private static final JsonWriterSettings EXTENDED_JSON =
      JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

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
  public void documentWithMetadata_toDlqJson_usesOriginalDocumentAndPreservesCustomerDataField() {
    Document doc = new Document("_id", 1).append("data", "customer_business_payload");
    DocumentWithMetadata original =
        DocumentWithMetadata.of(
            doc, doc.toJson(), 0, "Error message", RETRYABLE, "srcCol", "tgtCol");

    String dlqJson = original.toDlqJson("Error message", RETRYABLE, 1);
    assertTrue(
        "DLQ JSON should store document under _original_document",
        dlqJson.contains("\"_original_document\":"));

    DocumentWithMetadata reconstructed = DocumentWithMetadata.fromDlqJson(dlqJson);
    assertEquals("customer_business_payload", reconstructed.getDocument().getString("data"));
    assertEquals(original.getDocument(), reconstructed.getDocument());
  }

  @Test
  public void documentWithMetadata_fromDlqJson_metadataOriginalDocumentFallback_success() {
    String intermediateDlqJson =
        "{\"_metadata_original_document\":{\"_id\":1,\"name\":\"intermediate_test\"},\"_metadata_error_message\":\"Error\",\"_metadata_error_type\":\"RETRYABLE\",\"_metadata_retry_count\":1}";
    DocumentWithMetadata reconstructed = DocumentWithMetadata.fromDlqJson(intermediateDlqJson);

    assertEquals(1, reconstructed.getDocument().get("_id"));
    assertEquals("intermediate_test", reconstructed.getDocument().getString("name"));
  }

  @Test
  public void documentWithMetadata_fromDlqJson_legacyDataFieldFallback_success() {
    String legacyDlqJson =
        "{\"data\":{\"_id\":1,\"name\":\"legacy_test\"},\"_metadata_error_message\":\"Error\",\"_metadata_error_type\":\"RETRYABLE\",\"_metadata_retry_count\":1}";
    DocumentWithMetadata reconstructed = DocumentWithMetadata.fromDlqJson(legacyDlqJson);

    assertEquals(1, reconstructed.getDocument().get("_id"));
    assertEquals("legacy_test", reconstructed.getDocument().getString("name"));
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

  @Test
  public void documentWithMetadata_cdcEvent_andDlqRoundTrip() {
    TimestampSortKey key = TimestampSortKey.cdc(1724000000L, 7L);
    DocumentWithMetadata cdcEvent =
        DocumentWithMetadata.cdcEvent(
            null,
            null,
            "srcCol",
            "tgtCol",
            DocumentWithMetadata.OperationType.DELETE,
            key,
            "{\"_id\": 999}");

    assertEquals(DocumentWithMetadata.OperationType.DELETE, cdcEvent.getOperationType());
    assertTrue(cdcEvent.getOperationType().isDelete());
    assertEquals(999, cdcEvent.getId());
    assertEquals(key, cdcEvent.getTimestampSortKey());

    String dlqJson = cdcEvent.toDlqJson("Delete target doc not found", RETRYABLE, 2);
    DocumentWithMetadata reconstructed = DocumentWithMetadata.fromDlqJson(dlqJson);

    assertEquals(DocumentWithMetadata.OperationType.DELETE, reconstructed.getOperationType());
    assertEquals("{\"_id\": 999}", reconstructed.getDocumentKey());
    assertEquals(999, reconstructed.getId());
    assertEquals(key, reconstructed.getTimestampSortKey());
    assertEquals(Integer.valueOf(2), reconstructed.getRetryCount());
    assertTrue(reconstructed.isDlqReconsumed());
  }

  @Test
  public void documentWithMetadata_withDocument_preservesCdcMetadata() {
    TimestampSortKey key = TimestampSortKey.cdc(1724000000L, 5L);
    Document originalDoc = new Document("_id", 100).append("val", "before");
    DocumentWithMetadata item =
        DocumentWithMetadata.cdcEvent(
            originalDoc,
            originalDoc.toJson(),
            "sourceCol",
            "targetCol",
            DocumentWithMetadata.OperationType.UPDATE,
            key,
            "{\"_id\": 100}");

    Document transformedDoc = new Document("_id", 100).append("val", "after");
    DocumentWithMetadata updated = item.withDocument(transformedDoc);

    assertEquals(transformedDoc, updated.getDocument());
    assertEquals(DocumentWithMetadata.OperationType.UPDATE, updated.getOperationType());
    assertEquals(key, updated.getTimestampSortKey());
    assertEquals("{\"_id\": 100}", updated.getDocumentKey());
    assertEquals("sourceCol", updated.getSourceCollection());
    assertEquals("targetCol", updated.getTargetCollection());
  }

  @Test
  public void getDedupKey_withoutId_producesDeterministicKey() {
    Document docWithoutId = new Document("name", "no_id_test").append("num", 42);
    DocumentWithMetadata item1 =
        DocumentWithMetadata.of(
            docWithoutId, docWithoutId.toJson(), 0, null, null, "sourceCol", "targetCol");
    DocumentWithMetadata item2 =
        DocumentWithMetadata.of(
            docWithoutId, docWithoutId.toJson(), 0, null, null, "sourceCol", "targetCol");

    assertEquals(item1.getDedupKey(), item2.getDedupKey());
    assertTrue(item1.getDedupKey().startsWith("targetCol#"));
  }

  @Test
  public void getDedupKey_shardedCdcAndBackfill_produceMatchingKeys() {
    ObjectId id = new ObjectId();
    Document doc = new Document("_id", id).append("customerId", 12345).append("name", "acme");

    DocumentWithMetadata backfill =
        DocumentWithMetadata.backfillEvent(
            doc, "srcCol", "tgtCol", TimestampSortKey.backfill(100L));

    // On a sharded collection the change stream documentKey also carries the shard key fields.
    String shardedDocumentKey =
        new Document("customerId", 12345).append("_id", id).toJson(EXTENDED_JSON);
    DocumentWithMetadata cdc =
        DocumentWithMetadata.cdcEvent(
            doc,
            doc.toJson(),
            "srcCol",
            "tgtCol",
            DocumentWithMetadata.OperationType.UPDATE,
            TimestampSortKey.cdc(100L, 1L),
            shardedDocumentKey);

    assertEquals(backfill.getDedupKey(), cdc.getDedupKey());
  }

  @Test
  public void getDedupKey_unshardedCdcAndBackfill_produceMatchingKeys() {
    ObjectId id = new ObjectId();
    Document doc = new Document("_id", id).append("name", "acme");

    DocumentWithMetadata backfill =
        DocumentWithMetadata.backfillEvent(
            doc, "srcCol", "tgtCol", TimestampSortKey.backfill(100L));

    String unshardedDocumentKey = new Document("_id", id).toJson(EXTENDED_JSON);
    DocumentWithMetadata cdc =
        DocumentWithMetadata.cdcEvent(
            doc,
            doc.toJson(),
            "srcCol",
            "tgtCol",
            DocumentWithMetadata.OperationType.UPDATE,
            TimestampSortKey.cdc(100L, 1L),
            unshardedDocumentKey);

    assertEquals(backfill.getDedupKey(), cdc.getDedupKey());
  }
}
