package com.google.cloud.teleport.v2.transforms;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.firestore.v1.DocumentRootName;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.CollectionSelector;
import com.google.firestore.v1.StructuredQuery.Direction;
import com.google.firestore.v1.StructuredQuery.FieldReference;
import com.google.firestore.v1.StructuredQuery.Order;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CreatePartitionQueryRequestFnTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static final String PROJECT_ID = "test-project";
  private static final String DATABASE_ID = "test-db";
  private static final String PARENT_PATH = DocumentRootName.of(PROJECT_ID, DATABASE_ID).toString();
  private static final int PARTITION_COUNT = 10;

  @Test
  public void testTransformSingleCollection() {
    String collectionId = "users";

    PCollection<String> input = p.apply(Create.of(collectionId));

    CreatePartitionQueryRequestFn fn =
        new CreatePartitionQueryRequestFn(PROJECT_ID, DATABASE_ID, PARTITION_COUNT);
    PCollection<PartitionQueryRequest> output = input.apply(fn);

    PartitionQueryRequest expected =
        PartitionQueryRequest.newBuilder()
            .setParent(PARENT_PATH)
            .setStructuredQuery(
                StructuredQuery.newBuilder()
                    .addFrom(
                        CollectionSelector.newBuilder()
                            .setCollectionId(collectionId)
                            .setAllDescendants(true))
                    .addOrderBy(
                        Order.newBuilder()
                            .setField(FieldReference.newBuilder().setFieldPath("__name__"))
                            .setDirection(Direction.ASCENDING)))
            .setPartitionCount(PARTITION_COUNT - 1) // Verifying the -1 behavior
            .build();

    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void testTransformMultipleCollections() {
    ImmutableList<String> collectionIds = ImmutableList.of("products", "orders", "reviews");

    PCollection<String> input = p.apply(Create.of(collectionIds));

    CreatePartitionQueryRequestFn fn =
        new CreatePartitionQueryRequestFn(PROJECT_ID, DATABASE_ID, PARTITION_COUNT);
    PCollection<PartitionQueryRequest> output = input.apply(fn);

    ImmutableList.Builder<PartitionQueryRequest> expectedList = ImmutableList.builder();
    for (String collectionId : collectionIds) {
      expectedList.add(
          PartitionQueryRequest.newBuilder()
              .setParent(PARENT_PATH)
              .setStructuredQuery(
                  StructuredQuery.newBuilder()
                      .addFrom(
                          CollectionSelector.newBuilder()
                              .setCollectionId(collectionId)
                              .setAllDescendants(true))
                      .addOrderBy(
                          Order.newBuilder()
                              .setField(FieldReference.newBuilder().setFieldPath("__name__"))
                              .setDirection(Direction.ASCENDING)))
              .setPartitionCount(PARTITION_COUNT - 1)
              .build());
    }

    PAssert.that(output).containsInAnyOrder(expectedList.build());
    p.run();
  }

  @Test
  public void testTransformWithDefaultDatabase() {
    String collectionId = "items";
    String defaultDbId = "(default)";
    String expectedParentPath = "projects/test-project/databases/(default)";

    PCollection<String> input = p.apply(Create.of(collectionId));

    CreatePartitionQueryRequestFn fn =
        new CreatePartitionQueryRequestFn(PROJECT_ID, defaultDbId, PARTITION_COUNT);
    PCollection<PartitionQueryRequest> output = input.apply(fn);

    PartitionQueryRequest expected =
        PartitionQueryRequest.newBuilder()
            .setParent(expectedParentPath)
            .setStructuredQuery(
                StructuredQuery.newBuilder()
                    .addFrom(
                        CollectionSelector.newBuilder()
                            .setCollectionId(collectionId)
                            .setAllDescendants(true))
                    .addOrderBy(
                        Order.newBuilder()
                            .setField(FieldReference.newBuilder().setFieldPath("__name__"))
                            .setDirection(Direction.ASCENDING)))
            .setPartitionCount(PARTITION_COUNT - 1)
            .build();

    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }
}
