/*
 * Copyright (C) 2022 Google LLC
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
package com.infusionsoft.dataflow.shared;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.storage.Storage;
import com.google.datastore.v1.Key;
import com.infusionsoft.dataflow.utils.CloudStorageUtils;
import com.infusionsoft.dataflow.utils.DatastoreUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.StringUtils;

public class DeleteEmailContent extends DoFn<Key, Key> {

  private final String projectId;
  private final String bucket;

  public DeleteEmailContent(String projectId, String bucket) {
    checkArgument(StringUtils.isNotBlank(projectId), "projectId must not be blank");
    checkArgument(StringUtils.isNotBlank(bucket), "bucket must not be blank");

    this.projectId = projectId;
    this.bucket = bucket;
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    final com.google.datastore.v1.Key key = context.element();
    final long id = DatastoreUtils.getId(key);

    final Storage storage = CloudStorageUtils.getStorage(projectId);

    CloudStorageUtils.delete(storage, bucket, id + ".json", id + ".html", id + ".txt");
    context.output(key);
  }
}
