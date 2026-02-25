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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.compute.v1.Error;
import com.google.cloud.compute.v1.Errors;
import com.google.cloud.compute.v1.Firewall;
import com.google.cloud.compute.v1.FirewallsClient;
import com.google.cloud.compute.v1.Operation;
import io.grpc.Status;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.After;
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
  @Mock private OperationFuture<Operation, Operation> mockInsertOperationFuture;
  @Mock private OperationFuture<Operation, Operation> mockDeleteOperationFuture;
  private MockedStatic<FirewallsClient> mockedFirewallsClient;
  private NetworkFailureInjector injector;

  private static final String PROJECT_ID = "test-project";
  private static final String NETWORK_NAME = "test-network";
  private static final String TARGET_TAG = "test-tag";
  private static final int PORT = 8080;

  @Before
  public void setUp() throws Exception {
    mockedFirewallsClient = Mockito.mockStatic(FirewallsClient.class);
    mockedFirewallsClient.when(FirewallsClient::create).thenReturn(mockFirewallsClient);

    when(mockInsertOperationFuture.get()).thenReturn(Operation.newBuilder().build());
    when(mockFirewallsClient.insertAsync(anyString(), any(Firewall.class)))
        .thenReturn(mockInsertOperationFuture);
    when(mockFirewallsClient.deleteAsync(anyString(), anyString()))
        .thenReturn(mockDeleteOperationFuture);

    injector = NetworkFailureInjector.builder(PROJECT_ID, NETWORK_NAME).build();
  }

  @After
  public void tearDown() {
    mockedFirewallsClient.close();
  }

  @Test
  public void blockNetwork_success() throws ExecutionException, InterruptedException {
    injector.blockNetwork(TARGET_TAG, PORT);

    ArgumentCaptor<Firewall> firewallCaptor = ArgumentCaptor.forClass(Firewall.class);
    verify(mockFirewallsClient, times(2)).insertAsync(anyString(), firewallCaptor.capture());
    List<Firewall> capturedFirewalls = firewallCaptor.getAllValues();
    assertEquals(2, capturedFirewalls.size());

    Firewall ingressRule = capturedFirewalls.get(0);
    assertTrue(ingressRule.getName().contains("ingress"));
    assertEquals("INGRESS", ingressRule.getDirection());

    Firewall egressRule = capturedFirewalls.get(1);
    assertTrue(egressRule.getName().contains("egress"));
    assertEquals("EGRESS", egressRule.getDirection());
  }

  @Test
  public void cleanupAll_success() throws ExecutionException, InterruptedException {
    when(mockDeleteOperationFuture.get()).thenReturn(Operation.newBuilder().build());
    injector.blockNetwork(TARGET_TAG, PORT);
    injector.cleanupAll();

    ArgumentCaptor<String> deleteCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockFirewallsClient, times(2)).deleteAsync(anyString(), deleteCaptor.capture());
    List<String> deletedRules = deleteCaptor.getAllValues();
    assertEquals(2, deletedRules.size());
    assertTrue(deletedRules.get(0).contains("ingress"));
    assertTrue(deletedRules.get(1).contains("egress"));
    verify(mockFirewallsClient).close();
  }

  @Test
  public void blockNetwork_ingressCreationFails() throws Exception {
    Error error =
        Error.newBuilder().addErrors(Errors.newBuilder().setMessage("mock error")).build();
    Operation errorOperation = Operation.newBuilder().setError(error).build();
    when(mockInsertOperationFuture.get()).thenReturn(errorOperation);

    assertThrows(RuntimeException.class, () -> injector.blockNetwork(TARGET_TAG, PORT));

    ArgumentCaptor<String> deleteCaptor = ArgumentCaptor.forClass(String.class);
    // Verify that cleanup is called for the egress rule.
    verify(mockFirewallsClient, times(1)).deleteAsync(anyString(), deleteCaptor.capture());
    assertTrue(deleteCaptor.getValue().contains("egress"));
  }

  @Test
  public void blockNetwork_egressCreationFails() throws Exception {
    Operation successOperation = Operation.newBuilder().build();
    Error error =
        Error.newBuilder().addErrors(Errors.newBuilder().setMessage("mock error")).build();
    Operation errorOperation = Operation.newBuilder().setError(error).build();
    // Ingress succeeds, egress fails
    when(mockInsertOperationFuture.get()).thenReturn(successOperation).thenReturn(errorOperation);

    assertThrows(RuntimeException.class, () -> injector.blockNetwork(TARGET_TAG, PORT));

    ArgumentCaptor<String> deleteCaptor = ArgumentCaptor.forClass(String.class);
    // Verify that cleanup is called for the ingress rule.
    verify(mockFirewallsClient, times(1)).deleteAsync(anyString(), deleteCaptor.capture());
    assertTrue(deleteCaptor.getValue().contains("ingress"));
  }

  @Test
  public void cleanupAll_deletionFails() throws Exception {
    Error error =
        Error.newBuilder().addErrors(Errors.newBuilder().setMessage("mock error")).build();
    Operation errorOperation = Operation.newBuilder().setError(error).build();

    // Mock the delete operations to return an error
    when(mockDeleteOperationFuture.get()).thenReturn(errorOperation);

    injector.blockNetwork(TARGET_TAG, PORT);
    // This should not throw an exception, but log errors.
    injector.cleanupAll();

    verify(mockFirewallsClient, times(2)).deleteAsync(anyString(), anyString());
  }

  @Test
  public void cleanupAll_handlesNotFoundException() throws Exception {
    NotFoundException notFoundException =
        (NotFoundException)
            ApiExceptionFactory.createException(
                "mocked not found", null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false);
    ExecutionException notFound = new ExecutionException(notFoundException);
    when(mockDeleteOperationFuture.get()).thenThrow(notFound);

    injector.blockNetwork(TARGET_TAG, PORT);
    // This should not throw an exception, but log info.
    injector.cleanupAll();

    verify(mockFirewallsClient, times(2)).deleteAsync(anyString(), anyString());
  }
}
