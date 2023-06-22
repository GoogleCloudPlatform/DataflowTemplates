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
package com.google.cloud.teleport.it.gcp.datastream;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.datastream.v1.ConnectionProfile;
import com.google.cloud.datastream.v1.CreateConnectionProfileRequest;
import com.google.cloud.datastream.v1.CreateStreamRequest;
import com.google.cloud.datastream.v1.DatastreamClient;
import com.google.cloud.datastream.v1.DeleteConnectionProfileRequest;
import com.google.cloud.datastream.v1.DeleteStreamRequest;
import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.OperationMetadata;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.datastream.v1.UpdateStreamRequest;
import com.google.protobuf.Empty;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit test for {@link DatastreamResourceManager}. */
@RunWith(JUnit4.class)
public class DatastreamResourceManagerTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  private static final String CONNECTION_PROFILE_ID = "test-connection-profile-id";
  private static final String STREAM_ID = "test-stream-id";
  private static final String TEST_ID = "test-id";
  private static final String PROJECT_ID = "test-project";
  private static final String LOCATION = "test-location";
  private static final String BUCKET = "test-bucket";
  private static final String STREAM_STATE = "test-stream-state";
  private static final int RESOURCE_COUNT = 5;

  @Mock private DatastreamClient datastreamClient;

  @Mock
  private OperationFuture<ConnectionProfile, OperationMetadata> createConnectionProfileRequest;

  @Mock private OperationFuture<Stream, OperationMetadata> createStreamRequest;
  @Mock private OperationFuture<Stream, OperationMetadata> updateStreamRequest;
  @Mock private OperationFuture<Empty, OperationMetadata> deleteStreamRequest;
  @Mock private OperationFuture<Empty, OperationMetadata> deleteConnectionProfileRequest;
  @Mock private ConnectionProfile connectionProfile;
  @Mock private SourceConfig sourceConfig;
  @Mock private DestinationConfig destinationConfig;
  @Mock private Stream stream;
  @Mock private Stream.State streamState;

  private DatastreamResourceManager testManager;

  @Before
  public void setup() {
    testManager =
        new DatastreamResourceManager(
            datastreamClient, DatastreamResourceManager.builder(TEST_ID, PROJECT_ID, LOCATION));
  }

  @Test
  public void testCreateBQDestinationConnectionProfileExecutionExceptionShouldFail()
      throws ExecutionException, InterruptedException {
    when(datastreamClient.createConnectionProfileAsync(any(CreateConnectionProfileRequest.class)))
        .thenReturn(createConnectionProfileRequest);
    when(createConnectionProfileRequest.get()).thenThrow(ExecutionException.class);
    DatastreamResourceManagerException exception =
        assertThrows(
            DatastreamResourceManagerException.class,
            () -> testManager.createBQDestinationConnectionProfile(CONNECTION_PROFILE_ID));
    assertThat(exception)
        .hasMessageThat()
        .contains("Failed to create BQ destination connection profile.");
  }

  @Test
  public void testCreateBQDestinationConnectionInterruptedExceptionShouldFail()
      throws ExecutionException, InterruptedException {
    when(datastreamClient.createConnectionProfileAsync(any(CreateConnectionProfileRequest.class)))
        .thenReturn(createConnectionProfileRequest);
    when(createConnectionProfileRequest.get()).thenThrow(InterruptedException.class);
    DatastreamResourceManagerException exception =
        assertThrows(
            DatastreamResourceManagerException.class,
            () -> testManager.createBQDestinationConnectionProfile(CONNECTION_PROFILE_ID));
    assertThat(exception)
        .hasMessageThat()
        .contains("Failed to create BQ destination connection profile.");
  }

  @Test
  public void testCreateBQDestinationConnectionShouldCreateSuccessfully()
      throws ExecutionException, InterruptedException {
    when(datastreamClient.createConnectionProfileAsync(any(CreateConnectionProfileRequest.class)))
        .thenReturn(createConnectionProfileRequest);
    when(createConnectionProfileRequest.get()).thenReturn(connectionProfile);
    assertThat(testManager.createBQDestinationConnectionProfile(CONNECTION_PROFILE_ID))
        .isEqualTo(connectionProfile);
  }

  @Test
  public void testCreateGCSDestinationConnectionProfileExecutionExceptionShouldFail()
      throws ExecutionException, InterruptedException {
    when(datastreamClient.createConnectionProfileAsync(any(CreateConnectionProfileRequest.class)))
        .thenReturn(createConnectionProfileRequest);
    when(createConnectionProfileRequest.get()).thenThrow(ExecutionException.class);
    DatastreamResourceManagerException exception =
        assertThrows(
            DatastreamResourceManagerException.class,
            () -> testManager.createGCSDestinationConnectionProfile(CONNECTION_PROFILE_ID, BUCKET));
    assertThat(exception)
        .hasMessageThat()
        .contains("Failed to create GCS destination connection profile.");
  }

  @Test
  public void testCreateGCSDestinationConnectionProfileInterruptedExceptionShouldFail()
      throws ExecutionException, InterruptedException {
    when(datastreamClient.createConnectionProfileAsync(any(CreateConnectionProfileRequest.class)))
        .thenReturn(createConnectionProfileRequest);
    when(createConnectionProfileRequest.get()).thenThrow(InterruptedException.class);
    DatastreamResourceManagerException exception =
        assertThrows(
            DatastreamResourceManagerException.class,
            () -> testManager.createGCSDestinationConnectionProfile(CONNECTION_PROFILE_ID, BUCKET));
    assertThat(exception)
        .hasMessageThat()
        .contains("Failed to create GCS destination connection profile.");
  }

  @Test
  public void testCreateGCSDestinationConnectionShouldCreateSuccessfully()
      throws ExecutionException, InterruptedException {
    when(datastreamClient.createConnectionProfileAsync(any(CreateConnectionProfileRequest.class)))
        .thenReturn(createConnectionProfileRequest);
    when(createConnectionProfileRequest.get()).thenReturn(connectionProfile);
    assertThat(testManager.createGCSDestinationConnectionProfile(CONNECTION_PROFILE_ID, BUCKET))
        .isEqualTo(connectionProfile);
  }

  @Test
  public void testCreateStreamExecutionExceptionShouldFail()
      throws ExecutionException, InterruptedException {
    when(datastreamClient.createStreamAsync(any(CreateStreamRequest.class)))
        .thenReturn(createStreamRequest);
    when(createStreamRequest.get()).thenThrow(ExecutionException.class);
    DatastreamResourceManagerException exception =
        assertThrows(
            DatastreamResourceManagerException.class,
            () -> testManager.createStream(STREAM_ID, sourceConfig, destinationConfig));
    assertThat(exception).hasMessageThat().contains("Failed to create stream.");
  }

  @Test
  public void testCreateStreamInterruptedExceptionShouldFail()
      throws ExecutionException, InterruptedException {
    when(datastreamClient.createStreamAsync(any(CreateStreamRequest.class)))
        .thenReturn(createStreamRequest);
    when(createStreamRequest.get()).thenThrow(InterruptedException.class);
    DatastreamResourceManagerException exception =
        assertThrows(
            DatastreamResourceManagerException.class,
            () -> testManager.createStream(STREAM_ID, sourceConfig, destinationConfig));
    assertThat(exception).hasMessageThat().contains("Failed to create stream.");
  }

  @Test
  public void testCreateStreamShouldCreateSuccessfully()
      throws ExecutionException, InterruptedException {
    when(datastreamClient.createStreamAsync(any(CreateStreamRequest.class)))
        .thenReturn(createStreamRequest);
    when(createStreamRequest.get()).thenReturn(stream);
    assertThat(testManager.createStream(STREAM_ID, sourceConfig, destinationConfig))
        .isEqualTo(stream);
  }

  @Test
  public void testUpdateStreamStateInterruptedExceptionShouldFail()
      throws ExecutionException, InterruptedException {
    when(datastreamClient.updateStreamAsync(any(UpdateStreamRequest.class)))
        .thenReturn(updateStreamRequest);
    when(updateStreamRequest.get()).thenThrow(InterruptedException.class);
    when(streamState.name()).thenReturn(STREAM_STATE);
    DatastreamResourceManagerException exception =
        assertThrows(
            DatastreamResourceManagerException.class,
            () -> testManager.updateStreamState(stream, streamState));
    assertThat(exception).hasMessageThat().contains("Failed to update stream.");
  }

  @Test
  public void testUpdateStreamStateExecutionExceptionShouldFail()
      throws ExecutionException, InterruptedException {
    when(datastreamClient.updateStreamAsync(any(UpdateStreamRequest.class)))
        .thenReturn(updateStreamRequest);
    when(updateStreamRequest.get()).thenThrow(ExecutionException.class);
    when(streamState.name()).thenReturn(STREAM_STATE);
    DatastreamResourceManagerException exception =
        assertThrows(
            DatastreamResourceManagerException.class,
            () -> testManager.updateStreamState(stream, streamState));
    assertThat(exception).hasMessageThat().contains("Failed to update stream.");
  }

  @Test
  public void testUpdateStreamStateShouldCreateSuccessfully()
      throws ExecutionException, InterruptedException {
    Stream.Builder streamBuilder = mock(Stream.Builder.class, RETURNS_DEEP_STUBS);

    when(datastreamClient.updateStreamAsync(any(UpdateStreamRequest.class)))
        .thenReturn(updateStreamRequest);
    when(updateStreamRequest.get()).thenReturn(stream);
    when(stream.toBuilder()).thenReturn(streamBuilder);
    when(stream.getState()).thenReturn(streamState);
    when(streamState.name()).thenReturn(STREAM_STATE);
    assertThat(testManager.updateStreamState(stream, streamState).getState().name())
        .isEqualTo(STREAM_STATE);
  }

  @Test
  public void testCleanupAllShouldDeleteSuccessfullyWhenNoErrorIsThrown()
      throws ExecutionException, InterruptedException {

    when(datastreamClient.createConnectionProfileAsync(any(CreateConnectionProfileRequest.class)))
        .thenReturn(createConnectionProfileRequest);
    when(createConnectionProfileRequest.get()).thenReturn(connectionProfile);
    when(datastreamClient.createStreamAsync(any(CreateStreamRequest.class)))
        .thenReturn(createStreamRequest);
    when(createStreamRequest.get()).thenReturn(stream);

    when(datastreamClient.deleteStreamAsync(any(DeleteStreamRequest.class)))
        .thenReturn(deleteStreamRequest);
    when(datastreamClient.deleteConnectionProfileAsync((any(DeleteConnectionProfileRequest.class))))
        .thenReturn(deleteConnectionProfileRequest);

    for (int i = 0; i < RESOURCE_COUNT; i++) {
      testManager.createGCSDestinationConnectionProfile("gcs-" + CONNECTION_PROFILE_ID + i, BUCKET);
      testManager.createBQDestinationConnectionProfile("bq-" + CONNECTION_PROFILE_ID + i);
      testManager.createStream(STREAM_ID + i, sourceConfig, destinationConfig);
    }

    testManager.cleanupAll();
    verify(datastreamClient, times(RESOURCE_COUNT))
        .deleteStreamAsync(any(DeleteStreamRequest.class));
    verify(datastreamClient, times(RESOURCE_COUNT * 2))
        .deleteConnectionProfileAsync(any(DeleteConnectionProfileRequest.class));
    verify(datastreamClient).close();
  }
}
