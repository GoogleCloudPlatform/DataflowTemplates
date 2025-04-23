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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.teleport.v2.failureinjection.AlwaysFailPolicy;
import com.google.cloud.teleport.v2.spanner.service.SpannerService.GrpcErrorInjector;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientCall.Listener;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.MethodDescriptor.MethodType;
import org.junit.Test;

public class SpannerServiceTest {

  @Test
  public void testCreate() {
    SpannerService spannerService = new SpannerService("");
    SpannerOptions options = SpannerOptions.newBuilder().build();
    Spanner spanner = spannerService.create(options);
    assertNotNull(spanner);
  }

  @Test
  public void testIsCloudSpannerDataAPI() {
    GrpcErrorInjector errorInjector = new GrpcErrorInjector(null);

    assertTrue(errorInjector.isCloudSpannerDataAPI("google.spanner.v1.Spanner/StreamingRead"));
    assertTrue(errorInjector.isCloudSpannerDataAPI("google.spanner.v1.Spanner/BatchWrite"));
    assertTrue(errorInjector.isCloudSpannerDataAPI("google.spanner.v1.Spanner/Commit"));
    assertTrue(errorInjector.isCloudSpannerDataAPI("google.spanner.v1.Spanner/ExecuteBatchDml"));
    assertTrue(errorInjector.isCloudSpannerDataAPI("google.spanner.v1.Spanner/ExecuteSql"));
    assertTrue(
        errorInjector.isCloudSpannerDataAPI("google.spanner.v1.Spanner/ExecuteStreamingSql"));
    assertTrue(errorInjector.isCloudSpannerDataAPI("google.spanner.v1.Spanner/PartitionQuery"));
    assertTrue(errorInjector.isCloudSpannerDataAPI("google.spanner.v1.Spanner/PartitionRead"));
    assertTrue(errorInjector.isCloudSpannerDataAPI("google.spanner.v1.Spanner/Read"));

    assertFalse(
        errorInjector.isCloudSpannerDataAPI("google.spanner.v1.Spanner/BatchCreateSessions"));
    assertFalse(errorInjector.isCloudSpannerDataAPI("google.spanner.v1.Spanner/CreateSession"));
    assertFalse(errorInjector.isCloudSpannerDataAPI("google.storage.v2.Storage/GetObject"));
  }

  @Test
  public void testInterceptCallDoesNotInterceptOtherCalls() {
    MethodDescriptor methodDescriptor =
        getMockMethodDescriptor("google.storage.v2.Storage/GetObject");
    CallOptions callOptions = CallOptions.DEFAULT;
    Channel mockChannel = mock(Channel.class);
    ClientCall mockClientCall = mock(ClientCall.class);
    when(mockChannel.newCall(any(), any())).thenReturn(mockClientCall);
    AlwaysFailPolicy alwaysFailPolicy = new AlwaysFailPolicy(null);

    GrpcErrorInjector errorInjector = new GrpcErrorInjector(alwaysFailPolicy);

    assertEquals(
        mockClientCall, errorInjector.interceptCall(methodDescriptor, callOptions, mockChannel));
  }

  @Test
  public void testInterceptCallInterceptsSpannerCalls() {
    MethodDescriptor methodDescriptor = getMockMethodDescriptor("google.spanner.v1.Spanner/Commit");
    CallOptions callOptions = CallOptions.DEFAULT;
    Channel mockChannel = mock(Channel.class);
    ClientCall mockClientCall = mock(ClientCall.class);
    when(mockChannel.newCall(any(), any())).thenReturn(mockClientCall);
    AlwaysFailPolicy alwaysFailPolicy = new AlwaysFailPolicy(null);

    GrpcErrorInjector errorInjector = new GrpcErrorInjector(alwaysFailPolicy);

    ClientCall forwardingClientCall =
        errorInjector.interceptCall(methodDescriptor, callOptions, mockChannel);
    Listener respListner = mock(Listener.class);
    forwardingClientCall.start(respListner, new Metadata());
    assertTrue(
        forwardingClientCall instanceof ForwardingClientCall.SimpleForwardingClientCall<?, ?>);
    assertNotEquals(mockClientCall, forwardingClientCall);
  }

  private MethodDescriptor getMockMethodDescriptor(String methodName) {
    return MethodDescriptor.newBuilder()
        .setType(MethodType.UNARY)
        .setFullMethodName(methodName)
        .setRequestMarshaller(mock(Marshaller.class))
        .setResponseMarshaller(mock(Marshaller.class))
        .build();
  }
}
