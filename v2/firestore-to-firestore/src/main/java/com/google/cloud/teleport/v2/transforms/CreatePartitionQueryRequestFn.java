package com.google.cloud.teleport.v2.transforms;

import com.google.firestore.v1.DocumentRootName;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.CollectionSelector;
import com.google.firestore.v1.StructuredQuery.Direction;
import com.google.firestore.v1.StructuredQuery.FieldReference;
import com.google.firestore.v1.StructuredQuery.Order;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class CreatePartitionQueryRequestFn extends
    PTransform<PCollection<String>, PCollection<PartitionQueryRequest>> {

  private final String projectId;
  private final String databaseId;
  private final long partitionCount;

  public CreatePartitionQueryRequestFn(String projectId, String databaseId, long partitionCount) {
    this.projectId = projectId;
    this.databaseId = databaseId;
    this.partitionCount = partitionCount;
  }

  @Override
  public PCollection<PartitionQueryRequest> expand(PCollection<String> input) {
    return input.apply("CreatePartitionQueryRequests",
        ParDo.of(new DoFn<String, PartitionQueryRequest>() {
          @ProcessElement
          public void processElement(ProcessContext ctx) {
            String collectionId = ctx.element();
            StructuredQuery.Builder query = StructuredQuery.newBuilder()
                .addFrom(
                    CollectionSelector.newBuilder()
                        .setCollectionId(collectionId)
                        .setAllDescendants(true)
                )
                .addOrderBy(
                    Order.newBuilder()
                        .setField(FieldReference.newBuilder().setFieldPath("__name__").build())
                        .setDirection(Direction.ASCENDING)
                        .build()
                );
            PartitionQueryRequest request = PartitionQueryRequest.newBuilder()
                .setParent(DocumentRootName.of(projectId, databaseId).toString())
                .setPartitionCount(partitionCount)
                .setStructuredQuery(query)
                .build();
            ctx.output(request);
          }
        }));
  }
}
