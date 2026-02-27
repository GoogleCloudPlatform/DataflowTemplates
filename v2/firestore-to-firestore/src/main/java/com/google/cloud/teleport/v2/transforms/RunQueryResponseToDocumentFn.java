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
import com.google.firestore.v1.RunQueryResponse;
import org.apache.beam.sdk.transforms.DoFn;

/** DoFn to extract Documents from a RunQueryResponse. */
public class RunQueryResponseToDocumentFn extends DoFn<RunQueryResponse, Document> {
  public static final TupleTag<Document> DOCUMENT_TAG = new TupleTag<Document>() {};
  public static final TupleTag<String> ERROR_TAG = new TupleTag<String>() {};

  @ProcessElement
  public void processElement(ProcessContext c) {
    RunQueryResponse response = c.element();
    try {
      if (response != null && response.hasDocument()) {
        c.output(DOCUMENT_TAG, response.getDocument());
      } else {
        c.output(ERROR_TAG, "Response is null or has no document: " + response);
      }
    } catch (Exception e) {
      c.output(ERROR_TAG, "Exception while processing RunQueryResponse: " + e.getMessage());
    }
  }
}
