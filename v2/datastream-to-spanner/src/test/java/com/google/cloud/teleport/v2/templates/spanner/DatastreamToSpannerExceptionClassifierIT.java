/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.spanner;

import static com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase.createSpannerDDL;
import static com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase.executeSpannerDML;
import static com.google.cloud.teleport.v2.templates.spanner.DatastreamToSpannerExceptionClassifier.ErrorTag.PERMANENT_ERROR;
import static com.google.cloud.teleport.v2.templates.spanner.DatastreamToSpannerExceptionClassifier.ErrorTag.RETRYABLE_ERROR;
import static com.google.cloud.teleport.v2.templates.spanner.DatastreamToSpannerExceptionClassifierTest.assertSpannerExceptionClassification;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.SpannerExceptionParser;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.spanner.DatastreamToSpannerExceptionClassifier.ErrorTag;
import java.io.IOException;
import java.util.List;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Integration test for {@link DatastreamToSpannerExceptionClassifier}. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
public class DatastreamToSpannerExceptionClassifierIT {

  public static SpannerResourceManager spannerResourceManager;

  protected static final String PROJECT = TestProperties.project();
  protected static final String REGION = TestProperties.region();
  private static final String SPANNER_DDL_RESOURCE =
      "SpannerExceptionClassifierIT/spanner-schema.sql";
  private static final String SPANNER_INSERT_STATEMENTS =
      "SpannerExceptionClassifierIT/spanner-data.sql";

  @Before
  public void setUp() throws IOException, InterruptedException {
    synchronized (DatastreamToSpannerExceptionClassifierIT.class) {
      if (spannerResourceManager == null) {
        spannerResourceManager =
            SpannerResourceManager.builder(
                    DatastreamToSpannerExceptionClassifierIT.class.getSimpleName(), PROJECT, REGION)
                .maybeUseStaticInstance()
                .build();
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
        executeSpannerDML(spannerResourceManager, SPANNER_INSERT_STATEMENTS);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void testInterleaveInParentInsertChildBeforeParent() {
    ErrorTag actualTag = null;
    SpannerException exception = null;
    Mutation mutation =
        Mutation.newInsertBuilder("Books")
            .set("id")
            .to(4)
            .set("author_id")
            .to(100)
            .set("title")
            .to("Child")
            .build();
    try {
      spannerResourceManager.writeInTransaction(List.of(mutation));
    } catch (SpannerException e) {
      exception = e;
      actualTag = DatastreamToSpannerExceptionClassifier.classify(SpannerExceptionParser.parse(e));
    }
    Assert.assertNotNull(exception);
    Assert.assertEquals(
        exception.getMessage(),
        "NOT_FOUND: io.grpc.StatusRuntimeException: NOT_FOUND: Parent row for row [100,4] in table Books is missing. Row cannot be written.");
    assertSpannerExceptionClassification(exception, RETRYABLE_ERROR, actualTag);
  }

  @Test
  public void testInterleaveInInsertChildBeforeParent() {
    ErrorTag actualTag = null;
    SpannerException exception = null;
    Mutation mutation =
        Mutation.newInsertBuilder("Series")
            .set("id")
            .to(4)
            .set("author_id")
            .to(100)
            .set("title")
            .to("Child")
            .build();
    try {
      spannerResourceManager.writeInTransaction(List.of(mutation));
    } catch (SpannerException e) {
      exception = e;
    }
    Assert.assertNull(exception);
  }

  @Test
  public void testFKInsertChildBeforeParent() {
    ErrorTag actualTag = null;
    SpannerException exception = null;
    Mutation mutation =
        Mutation.newInsertBuilder("ForeignKeyChild")
            .set("id")
            .to(4)
            .set("parent_id")
            .to(100)
            .build();
    try {
      spannerResourceManager.writeInTransaction(List.of(mutation));
    } catch (SpannerException e) {
      exception = e;
      actualTag = DatastreamToSpannerExceptionClassifier.classify(SpannerExceptionParser.parse(e));
    }
    Assert.assertNotNull(exception);
    Assert.assertEquals(
        exception.getMessage(),
        "FAILED_PRECONDITION: io.grpc.StatusRuntimeException: FAILED_PRECONDITION: Foreign key constraint `fk_constraint1` is violated on table `ForeignKeyChild`. Cannot find referenced values in ForeignKeyParent(id).");
    assertSpannerExceptionClassification(exception, RETRYABLE_ERROR, actualTag);
  }

  @Test
  public void testUniqueIndexError() {
    ErrorTag actualTag = null;
    SpannerException exception = null;
    Mutation mutation =
        Mutation.newInsertBuilder("Authors")
            .set("author_id")
            .to(10)
            .set("name")
            .to("J.R.R. Tolkien") // author already exists
            .build();
    try {
      spannerResourceManager.writeInTransaction(List.of(mutation));
    } catch (SpannerException e) {
      exception = e;
      actualTag = DatastreamToSpannerExceptionClassifier.classify(SpannerExceptionParser.parse(e));
    }
    Assert.assertNotNull(exception);
    Assert.assertEquals(
        exception.getMessage(),
        "ALREADY_EXISTS: io.grpc.StatusRuntimeException: ALREADY_EXISTS: Unique index violation on index idx_authors_name at index key [J.R.R. Tolkien,1]. It conflicts with row [1] in table Authors.");
    assertSpannerExceptionClassification(exception, PERMANENT_ERROR, actualTag);
  }

  @Test
  public void testCheckConstraintError() {
    ErrorTag actualTag = null;
    SpannerException exception = null;
    Mutation mutation =
        Mutation.newInsertBuilder("Authors")
            .set("author_id")
            .to(300) // check constraint < 200
            .set("name")
            .to("New Author")
            .build();
    try {
      spannerResourceManager.writeInTransaction(List.of(mutation));
    } catch (SpannerException e) {
      exception = e;
      actualTag = DatastreamToSpannerExceptionClassifier.classify(SpannerExceptionParser.parse(e));
    }
    Assert.assertNotNull(exception);
    Assert.assertEquals(
        exception.getMessage(),
        "OUT_OF_RANGE: io.grpc.StatusRuntimeException: OUT_OF_RANGE: Check constraint `Authors`.`check_author_id` is violated for key (300)");
    assertSpannerExceptionClassification(exception, PERMANENT_ERROR, actualTag);
  }

  @Test
  public void testTableNotFound() {
    ErrorTag actualTag = null;
    SpannerException exception = null;
    Mutation mutation =
        Mutation.newInsertBuilder("FakeTable")
            .set("author_id")
            .to(300)
            .set("name")
            .to("New Author")
            .build();
    try {
      spannerResourceManager.writeInTransaction(List.of(mutation));
    } catch (SpannerException e) {
      exception = e;
      actualTag = DatastreamToSpannerExceptionClassifier.classify(SpannerExceptionParser.parse(e));
    }
    Assert.assertNotNull(exception);
    Assert.assertEquals(
        exception.getMessage(),
        "NOT_FOUND: io.grpc.StatusRuntimeException: NOT_FOUND: Table not found: FakeTable\n"
            + "resource_type: \"spanner.googleapis.com/Table\"\n"
            + "resource_name: \"FakeTable\"\n"
            + "description: \"Table not found\"\n");
    assertSpannerExceptionClassification(exception, PERMANENT_ERROR, actualTag);
  }

  @Test
  public void testColumnNotFound() {
    ErrorTag actualTag = null;
    SpannerException exception = null;
    Mutation mutation =
        Mutation.newInsertBuilder("Authors")
            .set("author_id")
            .to(10)
            .set("name")
            .to("New Author")
            .set("FakeColumn")
            .to("FakeColumnValue")
            .build();
    try {
      spannerResourceManager.writeInTransaction(List.of(mutation));
    } catch (SpannerException e) {
      exception = e;
      actualTag = DatastreamToSpannerExceptionClassifier.classify(SpannerExceptionParser.parse(e));
    }
    Assert.assertNotNull(exception);
    Assert.assertEquals(
        exception.getMessage(),
        "NOT_FOUND: io.grpc.StatusRuntimeException: NOT_FOUND: Column not found in table Authors: FakeColumn\n"
            + "resource_type: \"spanner.googleapis.com/Column\"\n"
            + "resource_name: \"FakeColumn\"\n");
    assertSpannerExceptionClassification(exception, PERMANENT_ERROR, actualTag);
  }

  @Test
  public void testInterleavingInParentDeleteParentWhenChildExists() {
    ErrorTag actualTag = null;
    SpannerException exception = null;
    Mutation mutation = Mutation.delete("Authors", Key.of(1));
    try {
      spannerResourceManager.writeInTransaction(List.of(mutation));
    } catch (SpannerException e) {
      exception = e;
      actualTag = DatastreamToSpannerExceptionClassifier.classify(SpannerExceptionParser.parse(e));
    }
    Assert.assertNotNull(exception);
    Assert.assertEquals(
        exception.getMessage(),
        "FAILED_PRECONDITION: io.grpc.StatusRuntimeException: FAILED_PRECONDITION: Integrity constraint violation during DELETE/REPLACE. Found child row [1,1] in table Books.");
    assertSpannerExceptionClassification(exception, RETRYABLE_ERROR, actualTag);
  }

  @Test
  public void testFKDeleteParentWhenChildExists() {
    ErrorTag actualTag = null;
    SpannerException exception = null;
    Mutation mutation = Mutation.delete("ForeignKeyParent", Key.of(1));
    try {
      spannerResourceManager.writeInTransaction(List.of(mutation));
    } catch (SpannerException e) {
      exception = e;
      actualTag = DatastreamToSpannerExceptionClassifier.classify(SpannerExceptionParser.parse(e));
    }
    Assert.assertNotNull(exception);
    Assert.assertEquals(
        exception.getMessage(),
        "FAILED_PRECONDITION: io.grpc.StatusRuntimeException: FAILED_PRECONDITION: Foreign key constraint violation when deleting or updating referenced row(s): referencing row(s) found in table `ForeignKeyChild`.");
    assertSpannerExceptionClassification(exception, RETRYABLE_ERROR, actualTag);
  }

  @Test
  public void testFKUpdateParentWhenChildExists() {
    ErrorTag actualTag = null;
    SpannerException exception = null;
    Mutation mutation =
        Mutation.newUpdateBuilder("ForeignKeyParent")
            .set("id")
            .to(1)
            .set("name")
            .to("parent20")
            .build();
    try {
      spannerResourceManager.writeInTransaction(List.of(mutation));
    } catch (SpannerException e) {
      exception = e;
      actualTag = DatastreamToSpannerExceptionClassifier.classify(SpannerExceptionParser.parse(e));
    }
    Assert.assertNotNull(exception);
    Assert.assertEquals(
        exception.getMessage(),
        "FAILED_PRECONDITION: io.grpc.StatusRuntimeException: FAILED_PRECONDITION: Foreign key constraint violation when deleting or updating referenced row(s): referencing row(s) found in table `ForeignKeyChild`.");
    assertSpannerExceptionClassification(exception, RETRYABLE_ERROR, actualTag);
  }

  @Test
  public void datatypeMismatch() {
    ErrorTag actualTag = null;
    SpannerException exception = null;
    Mutation mutation =
        Mutation.newInsertBuilder("Books")
            .set("id")
            .to(1.5)
            .set("author_id")
            .to(1)
            .set("title")
            .to("New Book")
            .build();
    try {
      spannerResourceManager.writeInTransaction(List.of(mutation));
    } catch (SpannerException e) {
      exception = e;
      actualTag = DatastreamToSpannerExceptionClassifier.classify(SpannerExceptionParser.parse(e));
    }
    Assert.assertNotNull(exception);
    Assert.assertEquals(
        exception.getMessage(),
        "FAILED_PRECONDITION: io.grpc.StatusRuntimeException: FAILED_PRECONDITION: Invalid value for column id in table Books: Expected INT64.");
    assertSpannerExceptionClassification(exception, PERMANENT_ERROR, actualTag);
  }

  @Test
  public void insertNullInNonNullColumn() {
    ErrorTag actualTag = null;
    SpannerException exception = null;
    Mutation mutation =
        Mutation.newInsertBuilder("Books")
            .set("id")
            .to(6)
            .set("author_id")
            .to(1)
            .set("title")
            .to(Value.string(null))
            .build();
    try {
      spannerResourceManager.writeInTransaction(List.of(mutation));
    } catch (SpannerException e) {
      exception = e;
      actualTag = DatastreamToSpannerExceptionClassifier.classify(SpannerExceptionParser.parse(e));
    }
    Assert.assertNotNull(exception);
    Assert.assertEquals(
        exception.getMessage(),
        "FAILED_PRECONDITION: io.grpc.StatusRuntimeException: FAILED_PRECONDITION: title must not be NULL in table Books.");
    assertSpannerExceptionClassification(exception, PERMANENT_ERROR, actualTag);
  }

  @Test
  public void writeToStoredGeneratedColumn() {
    ErrorTag actualTag = null;
    SpannerException exception = null;
    Mutation mutation =
        Mutation.newInsertBuilder("Books")
            .set("id")
            .to(10)
            .set("author_id")
            .to(1)
            .set("title")
            .to("NEW BOOK")
            .set("titleLowerStored")
            .to("new book")
            .build();
    try {
      spannerResourceManager.writeInTransaction(List.of(mutation));
    } catch (SpannerException e) {
      exception = e;
      actualTag = DatastreamToSpannerExceptionClassifier.classify(SpannerExceptionParser.parse(e));
    }
    Assert.assertNotNull(exception);
    Assert.assertEquals(
        exception.getMessage(),
        "FAILED_PRECONDITION: io.grpc.StatusRuntimeException: FAILED_PRECONDITION: Cannot write into generated column `Books.titleLowerStored`.");
    assertSpannerExceptionClassification(exception, PERMANENT_ERROR, actualTag);
  }

  @Test
  public void pkValueOrDependantColValueNotProvidedForGenPKWhileUpdating() {
    ErrorTag actualTag = null;
    SpannerException exception = null;
    Mutation mutation =
        Mutation.newUpdateBuilder("GenPK").set("id1").to(1).set("name").to("Another Name").build();
    try {
      spannerResourceManager.writeInTransaction(List.of(mutation));
    } catch (SpannerException e) {
      exception = e;
      actualTag = DatastreamToSpannerExceptionClassifier.classify(SpannerExceptionParser.parse(e));
    }
    Assert.assertNotNull(exception);
    Assert.assertEquals(
        exception.getMessage(),
        "FAILED_PRECONDITION: io.grpc.StatusRuntimeException: FAILED_PRECONDITION: For an Update, the value of a generated primary key `id2` must be explicitly specified, or else its non-key dependent column `part1` must be specified. Key: [1,<default>]");
    assertSpannerExceptionClassification(exception, PERMANENT_ERROR, actualTag);
  }

  @Test
  public void deleteWithPartialKey() {
    ErrorTag actualTag = null;
    SpannerException exception = null;
    Mutation mutation = Mutation.delete("MultiKeyTable", Key.of(1));
    try {
      spannerResourceManager.writeInTransaction(List.of(mutation));
    } catch (SpannerException e) {
      exception = e;
      actualTag = DatastreamToSpannerExceptionClassifier.classify(SpannerExceptionParser.parse(e));
    }
    Assert.assertNotNull(exception);
    Assert.assertEquals(
        exception.getMessage(),
        "FAILED_PRECONDITION: io.grpc.StatusRuntimeException: FAILED_PRECONDITION: Wrong number of key parts for MultiKeyTable. Expected: 3. Got: [\"1\"]");
    assertSpannerExceptionClassification(exception, PERMANENT_ERROR, actualTag);
  }
}
