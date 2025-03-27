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

import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.spi.v1.SpannerInterceptorProvider;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.cloud.teleport.v2.failureInjection.ErrorInjectionPolicyFactory;
import com.google.cloud.teleport.v2.failureInjection.ErrorInjectionPolicy;

public class SpannerService
    implements ServiceFactory<Spanner, SpannerOptions>, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerService.class);

  private ErrorInjectionPolicy errorInjectionPolicy;

  /** Injects errors in streaming calls to simulate call restarts. */
  private static class GrpcErrorInjector implements ClientInterceptor, Serializable {

    private ErrorInjectionPolicy errorInjectionPolicy;
    private final Random random = new Random();

    GrpcErrorInjector(ErrorInjectionPolicy errorInjectionPolicy) {
      this.errorInjectionPolicy = errorInjectionPolicy;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        final MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      // Only inject errors in the Cloud Spanner data API.
      if (!method.getFullMethodName().startsWith("google.spanner.v1.Spanner")) {
        return next.newCall(method, callOptions);
      }
      if (method.getFullMethodName().startsWith("google.spanner.v1.Spanner/BatchCreateSessions")
          || method.getFullMethodName().startsWith("google.spanner.v1.Spanner/CreateSession")) {
        return next.newCall(method, callOptions);
      }

      final AtomicBoolean errorInjected = new AtomicBoolean();
      final ClientCall<ReqT, RespT> clientCall = next.newCall(method, callOptions);

      return new SimpleForwardingClientCall<ReqT, RespT>(clientCall) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          super.start(
              new SimpleForwardingClientCallListener<RespT>(responseListener) {
                @Override
                public void onMessage(RespT message) {
                  super.onMessage(message);
                  if (errorInjectionPolicy.shouldInjectionError()) {
                    // Cancel the call after at least one response has been received.
                    // This will cause the call to terminate, then we can set UNAVAILABLE
                    // in the onClose() handler to cause a retry.
                    errorInjected.set(true);
                    clientCall.cancel("Cancelling call for injected error", null);
                  }
                }

                @Override
                public void onClose(Status status, Metadata metadata) {
                  if (errorInjected.get()) {
                    // UNAVAILABLE error will cause the call to retry.
                    status = Status.OUT_OF_RANGE.augmentDescription("INJECTED BY TEST");
                  }
                  super.onClose(status, metadata);
                }
              },
              headers);
        }
      };
    }
  }

  public SpannerService(String parameter) {
    errorInjectionPolicy = ErrorInjectionPolicyFactory.getErrorInjectionPolicy(parameter);
  }

  @Override
  public Spanner create(SpannerOptions spannerOptions) {
    SpannerInterceptorProvider interceptorProvider =
        SpannerInterceptorProvider.createDefault().with(new GrpcErrorInjector(errorInjectionPolicy));

    SpannerOptions.Builder builder = spannerOptions.toBuilder();
    builder.setInterceptorProvider(interceptorProvider);
    builder.setServiceFactory(null);

    return builder.build().getService();
  }
}
