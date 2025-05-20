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
package com.google.cloud.teleport.v2.spanner.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.teleport.v2.failureinjection.ErrorInjectionPolicy;
import com.google.cloud.teleport.v2.spanner.service.SpannerService.GrpcErrorInjector;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.Status;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SpannerServiceTest {

  @Mock private ErrorInjectionPolicy mockErrorInjectionPolicy;
  @Mock private Channel mockChannel;
  @Mock private ClientCall<Object, Object> mockOriginalClientCall;
  @Mock private ClientCall.Listener<Object> mockOriginalResponseListener;

  private MethodDescriptor<Object, Object> spannerMethodDescriptor;
  private MethodDescriptor<Object, Object> otherMethodDescriptor;
  private GrpcErrorInjector errorInjector;

  @Before
  public void setUp() {
    spannerMethodDescriptor = getMockMethodDescriptor("google.spanner.v1.Spanner/ExecuteSql");
    otherMethodDescriptor = getMockMethodDescriptor("google.some.OtherService/SomeMethod");

    when(mockChannel.newCall(any(MethodDescriptor.class), any(CallOptions.class)))
        .thenReturn(mockOriginalClientCall);
  }

  @Test
  public void testCreate_withDefaultNoOpPolicyFromEmptyParameter() {
    SpannerService spannerService = new SpannerService("");
    SpannerOptions options = SpannerOptions.newBuilder().build();
    Spanner spanner = spannerService.create(options);
    assertNotNull(spanner);
  }

  @Test
  public void testCreate_withSpecificPolicyFromParameter() {
    ObjectNode policyConfig = JsonNodeFactory.instance.objectNode();
    policyConfig.put("policyType", "AlwaysFailPolicy");
    policyConfig.put("policyInput", "Test failure message");
    String jsonParameter = policyConfig.toString();

    SpannerService spannerService = new SpannerService(jsonParameter);
    SpannerOptions options = SpannerOptions.newBuilder().build();
    Spanner spanner = spannerService.create(options);
    assertNotNull(spanner);
  }

  @Test
  public void testIsCloudSpannerDataAPI() {
    GrpcErrorInjector localErrorInjector = new GrpcErrorInjector(null);

    assertTrue(localErrorInjector.isCloudSpannerDataAPI("google.spanner.v1.Spanner/StreamingRead"));
    assertTrue(localErrorInjector.isCloudSpannerDataAPI("google.spanner.v1.Spanner/BatchWrite"));
    assertTrue(localErrorInjector.isCloudSpannerDataAPI("google.spanner.v1.Spanner/Commit"));

    assertFalse(
        localErrorInjector.isCloudSpannerDataAPI("google.spanner.v1.Spanner/BatchCreateSessions"));
    assertFalse(
        localErrorInjector.isCloudSpannerDataAPI("google.spanner.v1.Spanner/CreateSession"));
    assertFalse(localErrorInjector.isCloudSpannerDataAPI("google.storage.v2.Storage/GetObject"));
    assertFalse(
        localErrorInjector.isCloudSpannerDataAPI(
            "google.spanner.admin.database.v1.DatabaseAdmin/ListDatabases"));
  }

  @Test
  public void testInterceptCall_doesNotInterceptNonSpannerDataCalls() {
    errorInjector = new GrpcErrorInjector(mockErrorInjectionPolicy);

    ClientCall<Object, Object> returnedCall =
        errorInjector.interceptCall(otherMethodDescriptor, CallOptions.DEFAULT, mockChannel);

    assertEquals(
        "Should return the original ClientCall for non-Spanner methods",
        mockOriginalClientCall,
        returnedCall);
    verify(mockErrorInjectionPolicy, never()).shouldInjectionError();
  }

  @Test
  public void testInterceptCall_interceptsSpannerDataCalls_returnsForwardingCall() {
    errorInjector = new GrpcErrorInjector(mockErrorInjectionPolicy);

    ClientCall<Object, Object> returnedCall =
        errorInjector.interceptCall(spannerMethodDescriptor, CallOptions.DEFAULT, mockChannel);

    assertTrue(
        "Should return a ForwardingClientCall for Spanner methods",
        returnedCall instanceof ForwardingClientCall.SimpleForwardingClientCall<?, ?>);
    assertNotEquals(
        "Returned call should not be the original ClientCall",
        mockOriginalClientCall,
        returnedCall);
  }

  @Test
  public void testInterceptCall_SpannerCall_onMessage_injectsErrorWhenPolicySaysYes() {
    when(mockErrorInjectionPolicy.shouldInjectionError()).thenReturn(true);
    errorInjector = new GrpcErrorInjector(mockErrorInjectionPolicy);

    ClientCall<Object, Object> forwardingCall =
        errorInjector.interceptCall(spannerMethodDescriptor, CallOptions.DEFAULT, mockChannel);

    // Capture the listener passed to the original call's start method
    ArgumentCaptor<ClientCall.Listener<Object>> listenerCaptor =
        ArgumentCaptor.forClass(ClientCall.Listener.class);
    forwardingCall.start(mockOriginalResponseListener, new Metadata());
    // The forwardingCall.start ultimately calls super.start (which is mockOriginalClientCall.start)
    // with a new SimpleForwardingClientCallListener.
    // We need to verify the behavior of THIS SimpleForwardingClientCallListener.

    // To test the listener logic, we need to get the actual listener instance
    // that was created inside interceptCall. The forwardingCall's start method
    // passes its own wrapped listener to the original call.
    verify(mockOriginalClientCall).start(listenerCaptor.capture(), any(Metadata.class));
    ClientCall.Listener<Object> wrappedListener = listenerCaptor.getValue();

    // Simulate onMessage being called on the wrapped listener
    Object fakeMessage = new Object();
    wrappedListener.onMessage(fakeMessage);

    verify(mockOriginalClientCall).cancel(eq("Cancelling call for injected error"), any());
    // Verify the original listener still gets the message before cancellation effects
    verify(mockOriginalResponseListener).onMessage(fakeMessage);

    // Simulate onClose being called on the wrapped listener after error injection
    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    Metadata trailers = new Metadata();
    wrappedListener.onClose(
        Status.OK, trailers); // Original status doesn't matter here as it's overridden

    verify(mockOriginalResponseListener).onClose(statusCaptor.capture(), eq(trailers));
    assertEquals(Status.Code.DEADLINE_EXCEEDED, statusCaptor.getValue().getCode());
    assertTrue(statusCaptor.getValue().getDescription().contains("INJECTED BY TEST"));
  }

  @Test
  public void testInterceptCall_SpannerCall_onMessage_doesNotInjectErrorWhenPolicySaysNo() {
    when(mockErrorInjectionPolicy.shouldInjectionError()).thenReturn(false);
    errorInjector = new GrpcErrorInjector(mockErrorInjectionPolicy);

    ClientCall<Object, Object> forwardingCall =
        errorInjector.interceptCall(spannerMethodDescriptor, CallOptions.DEFAULT, mockChannel);

    verify(mockOriginalClientCall, never()).cancel(any(String.class), any(Throwable.class));

    ArgumentCaptor<ClientCall.Listener<Object>> listenerCaptor =
        ArgumentCaptor.forClass(ClientCall.Listener.class);
    forwardingCall.start(mockOriginalResponseListener, new Metadata());
    verify(mockOriginalClientCall).start(listenerCaptor.capture(), any(Metadata.class));
    ClientCall.Listener<Object> wrappedListener = listenerCaptor.getValue();

    Object fakeMessage = new Object();
    wrappedListener.onMessage(fakeMessage);

    verify(mockOriginalClientCall, never()).cancel(any(String.class), any(Throwable.class));
    verify(mockOriginalResponseListener).onMessage(fakeMessage);

    // Simulate onClose with an original status
    Status originalStatus = Status.OK.withDescription("Original success");
    Metadata trailers = new Metadata();
    wrappedListener.onClose(originalStatus, trailers);

    verify(mockOriginalResponseListener).onClose(eq(originalStatus), eq(trailers));
  }

  @Test
  public void testInterceptCall_SpannerCall_onClose_usesOriginalStatusIfNoErrorInjected() {
    // This covers the case where onMessage was not called or errorInjectionPolicy was false
    // and errorInjected boolean remains false.
    errorInjector = new GrpcErrorInjector(mockErrorInjectionPolicy);

    ClientCall<Object, Object> forwardingCall =
        errorInjector.interceptCall(spannerMethodDescriptor, CallOptions.DEFAULT, mockChannel);

    ArgumentCaptor<ClientCall.Listener<Object>> listenerCaptor =
        ArgumentCaptor.forClass(ClientCall.Listener.class);
    forwardingCall.start(mockOriginalResponseListener, new Metadata());
    verify(mockOriginalClientCall).start(listenerCaptor.capture(), any(Metadata.class));
    ClientCall.Listener<Object> wrappedListener = listenerCaptor.getValue();

    // Directly simulate onClose without onMessage triggering an error
    Status originalStatus = Status.UNAVAILABLE.withDescription("Network issue");
    Metadata trailers = new Metadata();
    wrappedListener.onClose(originalStatus, trailers);

    verify(mockErrorInjectionPolicy, never()).shouldInjectionError();
    verify(mockOriginalResponseListener).onClose(eq(originalStatus), eq(trailers));
    verify(mockOriginalClientCall, never()).cancel(any(), any());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private MethodDescriptor<Object, Object> getMockMethodDescriptor(String methodName) {
    return MethodDescriptor.newBuilder()
        .setType(MethodType.UNARY)
        .setFullMethodName(methodName)
        .setRequestMarshaller(mock(Marshaller.class))
        .setResponseMarshaller(mock(Marshaller.class))
        .build();
  }
}
