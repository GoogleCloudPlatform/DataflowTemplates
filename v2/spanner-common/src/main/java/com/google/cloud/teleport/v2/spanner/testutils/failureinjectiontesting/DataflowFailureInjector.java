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
package com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.AutoscalingSettings;
import com.google.api.services.dataflow.model.Environment;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.WorkerPool;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.compute.v1.InstancesClient;
import com.google.cloud.compute.v1.Operation;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataflowFailureInjector {

  private static final Logger LOG = LoggerFactory.getLogger(DataflowFailureInjector.class);

  /**
   * Finds and Stops all worker VMs for a Dataflow job to simulate failure.
   *
   * @param projectId The Google Cloud project ID.
   * @param jobId The Dataflow job ID.
   */
  public static void abruptlyKillWorkers(String projectId, String jobId)
      throws IOException, ExecutionException, InterruptedException {

    try (InstancesClient instancesClient = InstancesClient.create()) {
      // Find all worker VMs for the job up to the specified limit.
      String filter = String.format("labels.dataflow_job_id = \"%s\"", jobId);

      // Store VMs to stop: Map<instanceName, zoneName>
      Map<String, String> vmsToStop = new HashMap<>();

      for (var entry : instancesClient.aggregatedList(projectId).iterateAll()) {
        // entry of key=Zone, value=Instance
        for (var instance : entry.getValue().getInstancesList()) {
          if (instance.getLabelsMap().containsKey("dataflow_job_id")
              && instance.getLabelsMap().get("dataflow_job_id").equals(jobId)) {
            vmsToStop.put(
                instance.getName(), entry.getKey().substring(entry.getKey().lastIndexOf('/') + 1));
          }
        }
      }

      if (vmsToStop.isEmpty()) {
        throw new RuntimeException("No worker VMs found to stop for job ID: " + jobId);
      }

      LOG.info("Found {} VMs to stop: {}", vmsToStop.size(), vmsToStop.keySet());

      // Stop each found VM. This is an asynchronous operation.
      for (Map.Entry<String, String> vmEntry : vmsToStop.entrySet()) {
        String instanceName = vmEntry.getKey();
        String zoneName = vmEntry.getValue();

        LOG.info("-> Stopping VM '{}' in zone '{}'...", instanceName, zoneName);
        OperationFuture<Operation, Operation> operation =
            instancesClient.stopAsync(projectId, zoneName, instanceName);

        // Block and wait for the stop operation to complete.
        Operation response = operation.get();

        if (response.hasError()) {
          LOG.error("Error stopping instance '{}': {}", instanceName, response.getError());
        } else {
          LOG.info("-> Stop of '{}' confirmed.", instanceName);
        }
      }
    }
  }

  /**
   * Updates the minimum number of workers for a Dataflow job.
   *
   * @param projectId The Google Cloud project ID.
   * @param region The Google Cloud region.
   * @param jobId The Dataflow job ID.
   * @param minNumWorkers The new minimum number of workers.
   */
  public static void updateMinNumWorkers(
      String projectId, String region, String jobId, int minNumWorkers) throws IOException {
    Dataflow dataflow =
        new Dataflow(
            Utils.getDefaultTransport(),
            Utils.getDefaultJsonFactory(),
            new HttpCredentialsAdapter(GoogleCredentials.getApplicationDefault()));

    Job job = dataflow.projects().locations().jobs().get(projectId, region, jobId).execute();

    if (job.getEnvironment() == null) {
      job.setEnvironment(new Environment());
    }
    if (job.getEnvironment().getWorkerPools() == null) {
      job.getEnvironment().setWorkerPools(Collections.singletonList(new WorkerPool()));
    }

    WorkerPool workerPool = job.getEnvironment().getWorkerPools().get(0);
    if (workerPool.getAutoscalingSettings() == null) {
      workerPool.setAutoscalingSettings(new AutoscalingSettings());
    }
    workerPool.getAutoscalingSettings().set("minNumWorkers", minNumWorkers);

    Job updateJob = new Job();
    updateJob.setEnvironment(new Environment());
    updateJob
        .getEnvironment()
        .setWorkerPools(
            Collections.singletonList(
                new WorkerPool()
                    .setAutoscalingSettings(
                        new AutoscalingSettings().set("minNumWorkers", minNumWorkers))));

    dataflow.projects().locations().jobs().update(projectId, region, jobId, updateJob).execute();
    LOG.info("Updated minNumWorkers to {} for job {}", minNumWorkers, jobId);
  }
}
