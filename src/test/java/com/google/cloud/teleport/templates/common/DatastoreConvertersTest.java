/*
 * Copyright (C) 2018 Google Inc.
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

package com.google.cloud.teleport.templates.common;

import com.google.cloud.teleport.templates.common.DatastoreConverters.CheckNoKey;
import com.google.cloud.teleport.templates.common.DatastoreConverters.CheckSameKey;
import com.google.cloud.teleport.templates.common.DatastoreConverters.EntityJsonPrinter;
import com.google.cloud.teleport.templates.common.DatastoreConverters.EntityToJson;
import com.google.cloud.teleport.templates.common.DatastoreConverters.EntityToSchemaJson;
import com.google.cloud.teleport.templates.common.DatastoreConverters.JsonToEntity;
import com.google.cloud.teleport.templates.common.DatastoreConverters.JsonToKey;
import com.google.cloud.teleport.templates.common.ErrorConverters.ErrorMessage;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Key.PathElement;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Value;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DatastoreConverters}. */
@RunWith(JUnit4.class)
public class DatastoreConvertersTest implements Serializable {
  private List<Entity> entities;
  private List<String> entitiesJson;

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Before
  public void testData() {
    entities = new ArrayList<>();
    entities.add(Entity.newBuilder()
        .setKey(Key.newBuilder()
            .setPartitionId(PartitionId.newBuilder()
                .setProjectId("my-project")
                .setNamespaceId("some-namespace"))
            .addPath(PathElement.newBuilder()
                .setId(1234)
                .setKind("monkey")))
        .putProperties("someString", Value.newBuilder()
            .setStringValue("someValue")
            .build())
        .build());
    entities.add(Entity.newBuilder()
        .setKey(Key.newBuilder()
            .setPartitionId(PartitionId.newBuilder()
                .setProjectId("my-project")
                .setNamespaceId("some-namespace"))
            .addPath(PathElement.newBuilder()
                .setName("SomeName")
                .setKind("monkey")))
        .putProperties("someString", Value.newBuilder()
            .setIntegerValue(100L)
            .build())
        .putProperties("someSubRecord", Value.newBuilder()
            .setEntityValue(Entity.newBuilder()
                .putProperties("someSubFloat", Value.newBuilder()
                    .setDoubleValue(0.234)
                    .build()))
            .build())
        .putProperties("someArray", Value.newBuilder()
            .setArrayValue(ArrayValue.newBuilder()
                .addValues(Value.newBuilder()
                    .setIntegerValue(1234L))
                .addValues(Value.newBuilder()
                    .setIntegerValue(1234L))
                .addValues(Value.newBuilder()
                    .setArrayValue(ArrayValue.newBuilder()
                        .addValues(Value.newBuilder()
                            .setStringValue("Some nested string in a nested array"))))
                .build())
            .build())
        .build());

    entitiesJson = new LinkedList<>();
    entitiesJson.add(
        "{\"key\":{\"partitionId\":{\"projectId\":\"my-project\","
            + "\"namespaceId\":\"some-namespace\"},\"path\":"
            + "[{\"kind\":\"monkey\",\"id\":\"1234\"}]},"
            + "\"properties\":{\"someString\":{\"stringValue\":\"someValue\"}}}");

  }

  /** Unit test for {@link DatastoreConverters.EntityToJson}. */
  @Test
  public void testEntityToJson() throws Exception {
    List<String> doFnJson = DoFnTester.of(new EntityToJson()).processBundle(entities);
    Assert.assertEquals(entitiesJson.get(0), doFnJson.get(0));
  }

  /** Unit test for {@link DatastoreConverters.JsonToEntity}. */
  @Test
  public void testJsonToEntity() throws Exception {
    // note that project id should not be set
    List<Entity> noProject = entities.stream().map(entity -> {
      return Entity.newBuilder()
          .setKey(Key.newBuilder()
              .setPartitionId(PartitionId.newBuilder()
                  .setNamespaceId(entity.getKey().getPartitionId().getNamespaceId())
                  .build())
              .addAllPath(entity.getKey().getPathList()))
          .putAllProperties(entity.getPropertiesMap())
          .build();
    }).collect(Collectors.toList());
    List<Entity> entities = DoFnTester.of(new JsonToEntity()).processBundle(entitiesJson);
    Assert.assertEquals(noProject.get(0), entities.get(0));
  }

  /** Unit test for {@link DatastoreConverters.JsonToKey}. */
  @Test
  public void testJsonToKey() throws Exception {
    List<Key> keys = DoFnTester.of(new JsonToKey()).processBundle(entitiesJson);
    Assert.assertEquals(entities.get(0).getKey(), keys.get(0));
  }

  /** Unit test for {@link DatastoreConverters.EntityToSchemaJson}. */
  @Test
  public void testEntityToSchemaJson() throws Exception {
    List<String> schemas = DoFnTester.of(new EntityToSchemaJson()).processBundle(entities);
    Assert.assertEquals("{\"someString\":\"STRING_VALUE\"}", schemas.get(0));
    Assert.assertEquals(
        "{\"someString\":\"INTEGER_VALUE\",\"someSubRecord\":"
            + "{\"someSubFloat\":\"DOUBLE_VALUE\"},\"someArray\":"
            + "[\"INTEGER_VALUE\",[\"STRING_VALUE\"]]}", schemas.get(1));

  }

  /** Unit test for {@link DatastoreConverters.CheckSameKey}. */
  @Test
  @Category(NeedsRunner.class)
  public void testCheckSameKey() throws Exception {
    Entity dupKeyEntity = Entity.newBuilder()
        .setKey(entities.get(0).getKey())
        .putProperties("SomeBSProp", Value.newBuilder().setStringValue("Some BS Value").build())
        .build();

    // copy all entities
    ArrayList<Entity> testEntitiesWithConflictKey  = new ArrayList<>(entities);

    // Add the duplicate entity at the end of the list
    testEntitiesWithConflictKey.add(dupKeyEntity);

    List<String> expectedErrors = new ArrayList<>();
    EntityJsonPrinter entityJsonPrinter = new EntityJsonPrinter();
    for (Entity e : Arrays.asList(entities.get(0), dupKeyEntity)) {
      expectedErrors.add(ErrorMessage.newBuilder()
          .setMessage("Duplicate Datastore Key")
          .setData(entityJsonPrinter.print(e))
          .build()
          .toJson());
    }

    TupleTag<Entity> goodTag = new TupleTag<Entity>("entities"){};
    TupleTag<String> errorTag = new TupleTag<String>("errors"){};

    PCollectionTuple results = pipeline
        .apply("Create", Create.of(testEntitiesWithConflictKey))
        .apply("RemoveDupKeys", CheckSameKey.newBuilder()
            .setGoodTag(goodTag)
            .setErrorTag(errorTag)
            .build());

    PAssert.that(results.get(goodTag)).containsInAnyOrder(entities.subList(1, entities.size()));
    PAssert.that(results.get(errorTag)).containsInAnyOrder(expectedErrors);

    pipeline.run();
  }

  /** Test {@link DatastoreConverters.CheckNoKey} with only correct entities. */
  @Test
  @Category(NeedsRunner.class)
  public void testCheckNoKeyAllCorrect() throws Exception {

    // Create test data
    List<Entity> testEntitiesWithKey = new ArrayList<>(entities);

    // Run the test
    TupleTag<Entity> successTag = new TupleTag<Entity>("entities") {};
    TupleTag<String> failureTag = new TupleTag<String>("failures") {};
    PCollectionTuple results =
        pipeline
            .apply("Create", Create.of(testEntitiesWithKey))
            .apply(
                "RemoveNoKeys",
                CheckNoKey.newBuilder()
                    .setSuccessTag(successTag)
                    .setFailureTag(failureTag)
                    .build());

    // Check the results
    PAssert.that(results.get(successTag)).containsInAnyOrder(testEntitiesWithKey);
    PAssert.that(results.get(failureTag)).empty();
    pipeline.run();
  }

  /** Test {@link DatastoreConverters.CheckNoKey} with only invalid entities. */
  @Test
  @Category(NeedsRunner.class)
  public void testCheckNoKeyAllInvalid() throws Exception {

    // Create test data
    List<Entity> testEntitiesWithNoKey = new ArrayList<>();
    List<String> expectedErrors = new ArrayList<>();
    EntityJsonPrinter entityJsonPrinter = new EntityJsonPrinter();
    for (int i = 0; i < entities.size(); i++) {
      Entity noKeyEntity =
          Entity.newBuilder()
              .putProperties("street", Value.newBuilder().setStringValue("Some street").build())
              .putProperties("number", Value.newBuilder().setIntegerValue(1L).build())
              .build();
      testEntitiesWithNoKey.add(noKeyEntity);
      expectedErrors.add(
          ErrorMessage.newBuilder()
              .setMessage("Datastore Entity Without Key")
              .setData(entityJsonPrinter.print(noKeyEntity))
              .build()
              .toJson());
    }

    // Run the test
    TupleTag<Entity> successTag = new TupleTag<Entity>("entities") {};
    TupleTag<String> failureTag = new TupleTag<String>("failures") {};
    PCollectionTuple results =
        pipeline
            .apply("Create", Create.of(testEntitiesWithNoKey))
            .apply(
                "RemoveNoKeys",
                CheckNoKey.newBuilder()
                    .setSuccessTag(successTag)
                    .setFailureTag(failureTag)
                    .build());

    // Check the results
    PAssert.that(results.get(successTag)).empty();
    PAssert.that(results.get(failureTag)).containsInAnyOrder(expectedErrors);
    pipeline.run();
  }

  /** Test {@link DatastoreConverters.CheckNoKey} with both correct and invalid entities. */
  @Test
  @Category(NeedsRunner.class)
  public void testCheckNoKeyBothCorrectAndInvalid() throws Exception {

    // Create test data
    List<Entity> testEntitiesWithNoKey = new ArrayList<>();
    List<String> expectedErrors = new ArrayList<>();
    EntityJsonPrinter entityJsonPrinter = new EntityJsonPrinter();
    for (int i = 0; i < entities.size(); i++) {
      Entity noKeyEntity =
          Entity.newBuilder()
              .putProperties("street", Value.newBuilder().setStringValue("Some street").build())
              .putProperties("number", Value.newBuilder().setIntegerValue(i).build())
              .build();
      testEntitiesWithNoKey.add(noKeyEntity);
      expectedErrors.add(
          ErrorMessage.newBuilder()
              .setMessage("Datastore Entity Without Key")
              .setData(entityJsonPrinter.print(noKeyEntity))
              .build()
              .toJson());
    }
    List<Entity> testEntities = new ArrayList<>(entities);
    testEntities.addAll(testEntitiesWithNoKey);

    // Run the test
    TupleTag<Entity> successTag = new TupleTag<Entity>("entities") {};
    TupleTag<String> failureTag = new TupleTag<String>("failures") {};
    PCollectionTuple results =
        pipeline
            .apply("Create", Create.of(testEntities))
            .apply(
                "RemoveNoKeys",
                CheckNoKey.newBuilder()
                    .setSuccessTag(successTag)
                    .setFailureTag(failureTag)
                    .build());

    // Check the results
    PAssert.that(results.get(successTag)).containsInAnyOrder(entities);
    PAssert.that(results.get(failureTag)).containsInAnyOrder(expectedErrors);
    pipeline.run();
  }
}
