/*
 * Copyright (C) 2018 Google LLC
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

package com.google.cloud.syndeo.service;

import com.google.cloud.syndeo.common.ProviderUtil;
import com.google.cloud.syndeo.service.v1.SyndeoServiceV1;
import com.google.cloud.syndeo.service.v1.TransformServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
// import org.apache.beam.sdk.expansion.service.ExpansionService;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyndeoTransformService extends TransformServiceGrpc.TransformServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(SyndeoTransformService.class);
  private static final Map<String, SchemaTransformProvider> TRANSFORM_PROVIDERS = new HashMap<>();
  private static final CountDownLatch READY_LATCH = new CountDownLatch(1);

  public static void main(String[] args) throws IOException, InterruptedException {
    int port = args.length > 0 ? Integer.parseInt(args[0]) : 12345;
    System.out.println("OIOI DUDE!");
    Server transformServer =
        ServerBuilder.forPort(port)
            .addService(new SyndeoTransformService())
            //                .addService(ProtoReflectionService.newInstance())
            .build();

    transformServer.start();
    //    ExpansionService beamService = new ExpansionService(args);
    loadSchemaTransforms();

    // The service is ready to run.
    READY_LATCH.countDown();

    try {
      transformServer.awaitTermination();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static void ready() {
    try {
      READY_LATCH.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Unexpected interruption while awaiting server initialization.", e);
    }
  }

  private static void loadSchemaTransforms() {
    for (SchemaTransformProvider p : ProviderUtil.getProviders()) {
      TRANSFORM_PROVIDERS.put(p.identifier(), p);
    }

    LOG.info("Loaded schema transforms for syndeo: {}", TRANSFORM_PROVIDERS);
  }

  @Override
  public void listTransforms(
      SyndeoServiceV1.ListTransformRequest request,
      StreamObserver<SyndeoServiceV1.ListTransformResponse> responseObserver) {
    ready();
  }

  @Override
  public void validateTransform(
      SyndeoServiceV1.ValidateTransformRequest request,
      StreamObserver<SyndeoServiceV1.ValidateTransformResponse> responseObserver) {
    ready();
  }
}
