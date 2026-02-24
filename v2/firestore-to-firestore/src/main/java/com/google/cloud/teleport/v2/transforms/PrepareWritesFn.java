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
    int documentsPathIndex = originalName.indexOf("/documents/");
    if (documentsPathIndex < 0) {
      throw new IllegalArgumentException("Invalid document name format: " + originalName);
    }
    String path = originalName.substring(documentsPathIndex + 1);
    String newName = String.format("projects/%s/databases/%s/%s", projectId, databaseId, path);

    Document newDoc = doc.toBuilder().setName(newName).build();
    c.output(Write.newBuilder().setUpdate(newDoc).build());
  }
}
