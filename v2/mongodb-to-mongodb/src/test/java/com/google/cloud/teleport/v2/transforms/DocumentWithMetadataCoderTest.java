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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.testing.CoderProperties;
import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DocumentWithMetadataCoder}. */
@RunWith(JUnit4.class)
public class DocumentWithMetadataCoderTest {

  @Test
  public void testEncodeDecode_fullObjectRoundTrip() throws IOException {
    Document doc = new Document("_id", "doc123").append("name", "Alice").append("age", 30);
    TimestampSortKey key = TimestampSortKey.cdc(1724000000L, 10L);

    DocumentWithMetadata original =
        new DocumentWithMetadata(
            doc,
            doc.toJson(),
            2,
            "Rate limit exceeded",
            DocumentWithMetadata.ErrorType.RETRYABLE,
            "sourceCol",
            "targetCol",
            DocumentWithMetadata.FailureStage.WRITE,
            DocumentWithMetadata.OperationType.UPDATE,
            key,
            "{\"_id\": \"doc123\"}",
            true);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DocumentWithMetadataCoder.of().encode(original, out);

    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    DocumentWithMetadata decoded = DocumentWithMetadataCoder.of().decode(in);

    assertNotNull(decoded);
    assertEquals("doc123", decoded.getId());
    assertEquals("Alice", decoded.getDocument().getString("name"));
    assertEquals(Integer.valueOf(30), decoded.getDocument().getInteger("age"));
    assertEquals(Integer.valueOf(2), decoded.getRetryCount());
    assertEquals("Rate limit exceeded", decoded.getErrorMessage());
    assertEquals(DocumentWithMetadata.ErrorType.RETRYABLE, decoded.getErrorType());
    assertEquals("sourceCol", decoded.getSourceCollection());
    assertEquals("targetCol", decoded.getTargetCollection());
    assertEquals(DocumentWithMetadata.FailureStage.WRITE, decoded.getFailureStage());
    assertEquals(DocumentWithMetadata.OperationType.UPDATE, decoded.getOperationType());
    assertEquals(key, decoded.getTimestampSortKey());
    assertEquals("{\"_id\": \"doc123\"}", decoded.getDocumentKey());
    assertTrue(decoded.isDlqReconsumed());
  }

  @Test
  public void testEncodeDecode_nullDocumentForDeleteEvent() throws IOException {
    TimestampSortKey key = TimestampSortKey.cdc(1724000000L, 5L);

    DocumentWithMetadata deleteEvent =
        DocumentWithMetadata.cdcEvent(
            null,
            null,
            "sourceCol",
            "targetCol",
            DocumentWithMetadata.OperationType.DELETE,
            key,
            "{\"_id\": \"docToDelete\"}");

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DocumentWithMetadataCoder.of().encode(deleteEvent, out);

    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    DocumentWithMetadata decoded = DocumentWithMetadataCoder.of().decode(in);

    assertNotNull(decoded);
    assertNull(decoded.getDocument());
    assertEquals(DocumentWithMetadata.OperationType.DELETE, decoded.getOperationType());
    assertEquals("{\"_id\": \"docToDelete\"}", decoded.getDocumentKey());
    assertEquals("docToDelete", decoded.getId());
    assertEquals(key, decoded.getTimestampSortKey());
  }

  @Test
  public void testEncodeDecode_nullHandling() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DocumentWithMetadataCoder.of().encode(null, out);

    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    DocumentWithMetadata decoded = DocumentWithMetadataCoder.of().decode(in);

    assertNull(decoded);
  }

  @Test
  public void testEncodeDecode_lazyOriginalDoc_omitsDuplicateString() throws IOException {
    Document doc = new Document("_id", 999).append("data", "some large text payload");
    DocumentWithMetadata item = DocumentWithMetadata.of(doc, "users", "users_target");

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DocumentWithMetadataCoder.of().encode(item, out);

    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    DocumentWithMetadata decoded = DocumentWithMetadataCoder.of().decode(in);

    assertNotNull(decoded);
    assertEquals(999, decoded.getId());
    assertEquals("some large text payload", decoded.getDocument().getString("data"));
    assertEquals(item.getOriginalDocument(), decoded.getOriginalDocument());
  }

  @Test
  public void testEncodeDecode_distinctOriginalDoc_afterUdfTransformation() throws IOException {
    Document originalDoc = new Document("_id", 10).append("count", 1);
    DocumentWithMetadata item = DocumentWithMetadata.of(originalDoc, "stats", "stats");

    // Simulate UDF transformation: doc is transformed, originalDoc is preserved
    Document transformedDoc = new Document("_id", 10).append("count", 2).append("udf", true);
    DocumentWithMetadata transformedItem = item.withDocument(transformedDoc);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DocumentWithMetadataCoder.of().encode(transformedItem, out);

    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    DocumentWithMetadata decoded = DocumentWithMetadataCoder.of().decode(in);

    assertNotNull(decoded);
    assertEquals(10, decoded.getId());
    assertEquals(Integer.valueOf(2), decoded.getDocument().getInteger("count"));
    assertEquals(true, decoded.getDocument().getBoolean("udf"));
    assertEquals(item.getOriginalDocument(), decoded.getOriginalDocument());
  }

  @Test
  public void testCoderProperties_deterministic() throws Exception {
    Document doc = new Document("_id", "123").append("k", "v");
    DocumentWithMetadata item1 = DocumentWithMetadata.of(doc, "s", "t");
    DocumentWithMetadata item2 = DocumentWithMetadata.of(doc, "s", "t");

    CoderProperties.coderDeterministic(DocumentWithMetadataCoder.of(), item1, item2);
  }
}
