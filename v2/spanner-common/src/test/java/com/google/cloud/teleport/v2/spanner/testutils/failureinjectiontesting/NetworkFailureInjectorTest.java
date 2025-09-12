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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.compute.v1.Firewall;
import com.google.cloud.compute.v1.FirewallsClient;
import com.google.cloud.compute.v1.Operation;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class NetworkFailureInjectorTest {

  @Mock private FirewallsClient mockFirewallsClient;
  @Mock private OperationFuture<Operation, Operation> mockOperationFuture;

  private static final String PROJECT_ID = "test-project";
  private static final String NETWORK_NAME = "test-network";
  private static final String TARGET_TAG = "test-tag";
  private static final int PORT = 8080;
  private static final Duration DURATION = Duration.ofSeconds(1);

  @Before
  public void setUp() throws Exception {
    when(mockOperationFuture.get()).thenReturn(Operation.newBuilder().build());
    when(mockFirewallsClient.insertAsync(anyString(), any(Firewall.class)))
        .thenReturn(mockOperationFuture);
    when(mockFirewallsClient.deleteAsync(anyString(), anyString())).thenReturn(mockOperationFuture);
  }

  @Test
  public void blockNetworkForHost_success()
      throws IOException, ExecutionException, InterruptedException {
    try (MockedStatic<FirewallsClient> mocked = Mockito.mockStatic(FirewallsClient.class)) {
      mocked.when(FirewallsClient::create).thenReturn(mockFirewallsClient);

      NetworkFailureInjector.blockNetworkForHost(
          PROJECT_ID, NETWORK_NAME, TARGET_TAG, PORT, DURATION);

      ArgumentCaptor<Firewall> firewallCaptor = ArgumentCaptor.forClass(Firewall.class);
      verify(mockFirewallsClient, Mockito.times(2))
          .insertAsync(anyString(), firewallCaptor.capture());
      List<Firewall> capturedFirewalls = firewallCaptor.getAllValues();
      assertEquals(2, capturedFirewalls.size());

      Firewall ingressRule = capturedFirewalls.get(0);
      assertTrue(ingressRule.getName().contains("ingress"));
      assertEquals("INGRESS", ingressRule.getDirection());

      Firewall egressRule = capturedFirewalls.get(1);
      assertTrue(egressRule.getName().contains("egress"));
      assertEquals("EGRESS", egressRule.getDirection());

      ArgumentCaptor<String> deleteCaptor = ArgumentCaptor.forClass(String.class);
      verify(mockFirewallsClient, Mockito.times(2))
          .deleteAsync(anyString(), deleteCaptor.capture());
      List<String> deletedRules = deleteCaptor.getAllValues();
      assertEquals(2, deletedRules.size());
      assertTrue(deletedRules.get(0).contains("ingress"));
      assertTrue(deletedRules.get(1).contains("egress"));
    }
  }

  @Test
  public void blockNetworkForHost_ingressCreationFails() throws Exception {
    Operation errorOperation =
        Operation.newBuilder()
            .setError(com.google.cloud.compute.v1.Error.newBuilder().build())
            .build();
    when(mockOperationFuture.get()).thenReturn(errorOperation);

    try (MockedStatic<FirewallsClient> mocked = Mockito.mockStatic(FirewallsClient.class)) {
      mocked.when(FirewallsClient::create).thenReturn(mockFirewallsClient);

      assertThrows(
          RuntimeException.class,
          () ->
              NetworkFailureInjector.blockNetworkForHost(
                  PROJECT_ID, NETWORK_NAME, TARGET_TAG, PORT, DURATION));

      ArgumentCaptor<String> deleteCaptor = ArgumentCaptor.forClass(String.class);
      // Verify that cleanup is called for the egress rule.
      verify(mockFirewallsClient, Mockito.times(1))
          .deleteAsync(anyString(), deleteCaptor.capture());
      assertTrue(deleteCaptor.getValue().contains("egress"));
    }
  }

  @Test
  public void blockNetworkForHost_egressCreationFails() throws Exception {
    Operation successOperation = Operation.newBuilder().build();
    Operation errorOperation =
        Operation.newBuilder()
            .setError(com.google.cloud.compute.v1.Error.newBuilder().build())
            .build();
    // Ingress succeeds, egress fails
    when(mockOperationFuture.get()).thenReturn(successOperation).thenReturn(errorOperation);

    try (MockedStatic<FirewallsClient> mocked = Mockito.mockStatic(FirewallsClient.class)) {
      mocked.when(FirewallsClient::create).thenReturn(mockFirewallsClient);

      assertThrows(
          RuntimeException.class,
          () ->
              NetworkFailureInjector.blockNetworkForHost(
                  PROJECT_ID, NETWORK_NAME, TARGET_TAG, PORT, DURATION));

      ArgumentCaptor<String> deleteCaptor = ArgumentCaptor.forClass(String.class);
      // Verify that cleanup is called for the ingress rule.
      verify(mockFirewallsClient, Mockito.times(1))
          .deleteAsync(anyString(), deleteCaptor.capture());
      assertTrue(deleteCaptor.getValue().contains("ingress"));
    }
  }

  @Test
  public void blockNetworkForHost_deletionFails() throws Exception {
    Operation successOperation = Operation.newBuilder().build();
    Operation errorOperation =
        Operation.newBuilder()
            .setError(com.google.cloud.compute.v1.Error.newBuilder().build())
            .build();

    // Mock the delete operations to return an error
    OperationFuture<Operation, Operation> mockDeleteOperationFuture = mock(OperationFuture.class);
    when(mockDeleteOperationFuture.get()).thenReturn(errorOperation);
    when(mockFirewallsClient.deleteAsync(anyString(), anyString()))
        .thenReturn(mockDeleteOperationFuture);

    try (MockedStatic<FirewallsClient> mocked = Mockito.mockStatic(FirewallsClient.class)) {
      mocked.when(FirewallsClient::create).thenReturn(mockFirewallsClient);

      // This should not throw an exception, but log errors.
      NetworkFailureInjector.blockNetworkForHost(
          PROJECT_ID, NETWORK_NAME, TARGET_TAG, PORT, DURATION);

      verify(mockFirewallsClient, Mockito.times(2)).deleteAsync(anyString(), anyString());
    }
  }
}
