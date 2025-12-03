package com.google.cloud.teleport.v2.transforms;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.Write;
import org.apache.beam.sdk.transforms.DoFn;

// DoFn to convert Document to Write requests for the destination database
public class PrepareWritesFn extends DoFn<Document, Write> {

  private final String projectId;
  private final String databaseId;

  public PrepareWritesFn(String projectId, String databaseId) {
    this.projectId = projectId;
    this.databaseId = databaseId;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    Document doc = c.element();
    // Rebuild the document name for the destination project/database
    String originalName = doc.getName();
    String path = originalName.substring(originalName.indexOf("/documents/") + 1);
    String newName = String.format("projects/%s/databases/%s/%s", projectId, databaseId, path);

    Document newDoc = doc.toBuilder().setName(newName).build();
    c.output(Write.newBuilder().setUpdate(newDoc).build());
  }
}
