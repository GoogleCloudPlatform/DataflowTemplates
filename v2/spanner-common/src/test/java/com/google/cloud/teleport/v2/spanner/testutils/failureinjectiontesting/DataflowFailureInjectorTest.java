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

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.cloud.compute.v1.Instance;
import com.google.cloud.compute.v1.InstancesClient;
import com.google.cloud.compute.v1.InstancesClient.AggregatedListPagedResponse;
import com.google.cloud.compute.v1.InstancesScopedList;
import com.google.cloud.compute.v1.Operation;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class DataflowFailureInjectorTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private InstancesClient mockInstancesClient;

  private static final String PROJECT_ID = "test-project";
  private static final String JOB_ID = "test-job-id";
  private static final String ZONE_1 = "us-central1-a";
  private static final String INSTANCE_1 = "gke-instance-1";

  @Test
  public void testAbruptlyKillWorkers_stopsCorrectVms()
      throws IOException, ExecutionException, InterruptedException {
    try (MockedStatic<InstancesClient> mocked = Mockito.mockStatic(InstancesClient.class)) {
      mocked.when(InstancesClient::create).thenReturn(mockInstancesClient);
      AggregatedListPagedResponse mockResponse = mock(AggregatedListPagedResponse.class);
      Instance instance =
          Instance.newBuilder().setName(INSTANCE_1).putLabels("dataflow_job_id", JOB_ID).build();
      InstancesScopedList scopedList =
          InstancesScopedList.newBuilder().addAllInstances(Arrays.asList(instance)).build();
      Map.Entry<String, InstancesScopedList> entry =
          new java.util.AbstractMap.SimpleEntry<>("zones/" + ZONE_1, scopedList);

      when(mockInstancesClient.aggregatedList(PROJECT_ID)).thenReturn(mockResponse);
      when(mockResponse.iterateAll()).thenReturn(Arrays.asList(entry));

      OperationFuture<Operation, Operation> mockFuture = mock(OperationFuture.class);
      Operation mockOperation = Operation.newBuilder().build();
      when(mockFuture.get()).thenReturn(mockOperation);
      when(mockInstancesClient.stopAsync(PROJECT_ID, ZONE_1, INSTANCE_1)).thenReturn(mockFuture);

      DataflowFailureInjector.abruptlyKillWorkers(PROJECT_ID, JOB_ID);

      verify(mockInstancesClient, times(1)).stopAsync(PROJECT_ID, ZONE_1, INSTANCE_1);
    }
  }

  @Test
  public void testAbruptlyKillWorkers_noVmsFound_throwsException()
      throws IOException, ExecutionException, InterruptedException {
    try (MockedStatic<InstancesClient> mocked = Mockito.mockStatic(InstancesClient.class)) {
      mocked.when(InstancesClient::create).thenReturn(mockInstancesClient);
      AggregatedListPagedResponse mockResponse = mock(AggregatedListPagedResponse.class);
      when(mockInstancesClient.aggregatedList(PROJECT_ID)).thenReturn(mockResponse);
      when(mockResponse.iterateAll()).thenReturn(Collections.emptyList());

      assertThrows(
          RuntimeException.class,
          () -> DataflowFailureInjector.abruptlyKillWorkers(PROJECT_ID, JOB_ID));
    }
  }

  @Test
  public void testAbruptlyKillWorkers_operationHasError_logsError()
      throws IOException, ExecutionException, InterruptedException {
    try (MockedStatic<InstancesClient> mocked = Mockito.mockStatic(InstancesClient.class)) {
      mocked.when(InstancesClient::create).thenReturn(mockInstancesClient);
      AggregatedListPagedResponse mockResponse = mock(AggregatedListPagedResponse.class);
      Instance instance =
          Instance.newBuilder().setName(INSTANCE_1).putLabels("dataflow_job_id", JOB_ID).build();
      InstancesScopedList scopedList =
          InstancesScopedList.newBuilder().addAllInstances(Arrays.asList(instance)).build();
      Map.Entry<String, InstancesScopedList> entry =
          new java.util.AbstractMap.SimpleEntry<>("zones/" + ZONE_1, scopedList);

      when(mockInstancesClient.aggregatedList(PROJECT_ID)).thenReturn(mockResponse);
      when(mockResponse.iterateAll()).thenReturn(Arrays.asList(entry));

      OperationFuture<Operation, Operation> mockFuture = mock(OperationFuture.class);
      Operation mockOperation =
          Operation.newBuilder()
              .setError(com.google.cloud.compute.v1.Error.newBuilder().build())
              .build();
      when(mockFuture.get()).thenReturn(mockOperation);
      when(mockInstancesClient.stopAsync(PROJECT_ID, ZONE_1, INSTANCE_1)).thenReturn(mockFuture);

      DataflowFailureInjector.abruptlyKillWorkers(PROJECT_ID, JOB_ID);

      verify(mockInstancesClient, times(1)).stopAsync(PROJECT_ID, ZONE_1, INSTANCE_1);
    }
  }

  @Test
  public void testUpdateMinNumWorkers() throws IOException {
    // Mock Dataflow client and related objects
    Dataflow mockDataflow = mock(Dataflow.class);
    Dataflow.Projects mockProjects = mock(Dataflow.Projects.class);
    Dataflow.Projects.Locations mockLocations = mock(Dataflow.Projects.Locations.class);
    Dataflow.Projects.Locations.Jobs mockJobs = mock(Dataflow.Projects.Locations.Jobs.class);
    Dataflow.Projects.Locations.Jobs.Get mockGet = mock(Dataflow.Projects.Locations.Jobs.Get.class);
    Dataflow.Projects.Locations.Jobs.Update mockUpdate =
        mock(Dataflow.Projects.Locations.Jobs.Update.class);

    when(mockDataflow.projects()).thenReturn(mockProjects);
    when(mockProjects.locations()).thenReturn(mockLocations);
    when(mockLocations.jobs()).thenReturn(mockJobs);
    when(mockJobs.get(anyString(), anyString(), anyString())).thenReturn(mockGet);
    when(mockJobs.update(anyString(), anyString(), anyString(), any(Job.class)))
        .thenReturn(mockUpdate);

    Job mockJob = new Job();
    when(mockGet.execute()).thenReturn(mockJob);
    when(mockUpdate.execute()).thenReturn(new Job());

    try (MockedConstruction<Dataflow> mockedDataflow =
        Mockito.mockConstruction(
            Dataflow.class,
            (mock, context) -> {
              when(mock.projects()).thenReturn(mockProjects);
            })) {

      DataflowFailureInjector.updateMinNumWorkers(PROJECT_ID, ZONE_1, JOB_ID, 5);

      verify(mockJobs, times(1)).get(PROJECT_ID, ZONE_1, JOB_ID);
      verify(mockJobs, times(1)).update(eq(PROJECT_ID), eq(ZONE_1), eq(JOB_ID), any(Job.class));
    }
  }
}
