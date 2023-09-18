/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.dfmetrics.output;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.teleport.dfmetrics.model.JobInfo;
import com.google.cloud.teleport.dfmetrics.utils.MetricsCollectorUtils;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class {@link FileStore} encapsulates the functionality to write the job metrics to File location.
 */
public class FileStore implements IOutputStore {
  private static final Logger LOG = LoggerFactory.getLogger(FileStore.class);

  private static final String GCS_URI = "gs";

  private final String filePath;

  public FileStore(String filePath) {
    this.filePath = filePath;
  }

  /**
   * Writes record to Output file location.
   *
   * @throws Exception
   */
  @Override
  public void load(JobInfo jobInfo, Map<String, Double> metrics) {
    LOG.info("Writing metrics to file path:\n{}", filePath);

    // Create the record from the provided details
    Map<String, Object> record = new HashMap<>();
    record.put("run_timestamp", Instant.now().toString());
    record.put("job_create_timestamp", jobInfo.createTime());
    record.put("sdk", jobInfo.sdk());
    record.put("sdk_version", jobInfo.sdkVersion());
    record.put("job_type", jobInfo.jobType());
    addIfValueExists(record, "template_name", jobInfo.templateName());
    addIfValueExists(record, "template_version", jobInfo.templateVersion());
    record.put("template_type", jobInfo.templateType());
    record.put("pipeline_name", jobInfo.pipelineName());
    record.put("parameters", jobInfo.parameters());
    record.put("metrics", metrics);

    String json = MetricsCollectorUtils.serializeToJson(record, true);
    byte[] contents = json.getBytes(StandardCharsets.UTF_8);

    // Write to file path
    try {
      URI uri = new URI(filePath);
      if (uri.getScheme() == null) { // Write to local file system
        Files.write(Paths.get(filePath), contents, StandardOpenOption.CREATE);
      } else if (uri.getScheme().equals(GCS_URI)) { // Write to Google cloud storage
        Storage storageClient = StorageOptions.getDefaultInstance().getService();
        BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.fromGsUtilUri(filePath)).build();
        storageClient.create(blobInfo, contents);
      } else {
        throw new RuntimeException(
            "Invalid file scheme. Supported FileSystems are local, Google Cloud Storage");
      }
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
