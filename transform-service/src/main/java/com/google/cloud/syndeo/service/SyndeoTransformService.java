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
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
// import org.apache.beam.sdk.expansion.service.ExpansionService;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyndeoTransformService extends TransformServiceGrpc.TransformServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(SyndeoTransformService.class);
  static final Map<String, SchemaTransformProvider> TRANSFORM_PROVIDERS = new HashMap<>();
  private static final CountDownLatch READY_LATCH = new CountDownLatch(1);

  public static void main(String[] args) throws IOException, InterruptedException {
    int port = args.length > 0 ? Integer.parseInt(args[0]) : 12345;
    System.out.println("OIOI DUDE!");
    Server transformServer =
        ServerBuilder.forPort(port)
            .addService(new SyndeoTransformService())
              .addService(ProtoReflectionService.newInstance())
            .build();

    transformServer.start();
    loadSchemaTransforms();

    // The service is ready to run.

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

  static void loadSchemaTransforms() {
    for (SchemaTransformProvider p : ProviderUtil.getProviders()) {
      TRANSFORM_PROVIDERS.put(p.identifier(), p);
    }

    LOG.info("Loaded schema transforms for syndeo: {}", TRANSFORM_PROVIDERS);
    READY_LATCH.countDown();
  }

  @Override
  public void listTransforms(
      SyndeoServiceV1.ListTransformRequest request,
      StreamObserver<SyndeoServiceV1.ListTransformResponse> responseObserver) {
    ready();
    SyndeoServiceV1.ListTransformResponse.Builder response =
            SyndeoServiceV1.ListTransformResponse.newBuilder();
    for (SchemaTransformProvider provider : TRANSFORM_PROVIDERS.values()) {
      try {
        response.addTransformSpecs(
                SyndeoServiceV1.TransformSpec.newBuilder()
                        .setUrn(provider.identifier())
                        .setConfigSchema(
                                SchemaTranslation.schemaToProto(provider.configurationSchema(), true))
                        .addAllExpectedInputs(provider.inputCollectionNames())
                        .build());
      } catch (Exception e) {
        LOG.error("Unable to include schema transform {} due to: {}", provider.identifier(), e);
      }
    }
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }

  @Override
  public void validateTransform(
      SyndeoServiceV1.ValidateTransformRequest request,
      StreamObserver<SyndeoServiceV1.ValidateTransformResponse> responseObserver) {
    ready();
    try {
      PipelineOptions options = PipelineOptionsFactory.create();
      options.setRunner(NotRunnableRunner.class);
      Pipeline p = Pipeline.create(options);
      PCollectionRowTuple inputs = PCollectionRowTuple.empty(p);
      for (Map.Entry<String, SchemaApi.Schema> input : request.getInputs().entrySet()) {
        inputs =
                inputs.and(
                        input.getKey(),
                        p.apply(
                                Create.empty(
                                        SchemaCoder.of(SchemaTranslation.schemaFromProto(input.getValue())))));
      }
      PCollectionRowTuple outputs =
              inputs.apply(createTransform(request.getTransform()));
      SyndeoServiceV1.ValidateTransformResponse.Builder response =
              SyndeoServiceV1.ValidateTransformResponse.newBuilder();
      for (Map.Entry<String, PCollection<Row>> output : outputs.getAll().entrySet()) {
        response.putOutputs(
                output.getKey(),
                SchemaTranslation.schemaToProto(output.getValue().getSchema(), true));
      }
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    } catch (RuntimeException exn) {
      responseObserver.onNext(
              SyndeoServiceV1.ValidateTransformResponse.newBuilder()
                      .setError(Throwables.getStackTraceAsString(exn))
                      .build());
      responseObserver.onCompleted();
    }
  }

  private PTransform<PCollectionRowTuple, PCollectionRowTuple> createTransform(
          SyndeoServiceV1.ConfiguredTransform config) {
    SchemaTransformProvider provider = TRANSFORM_PROVIDERS.get(config.getUrn());
    // TODO: Ensure config schemas are compatible.
    Row configRow;
    try {
      configRow = SchemaCoder.of(provider.configurationSchema())
              .decode(config.getConfigValues().newInput());
    } catch (IOException exn) {
      // This is more a decoding error than an IOException.
      throw new RuntimeException(exn);
    }
    return provider
            .from(configRow)
            .buildTransform();
  }


  private static class NotRunnableRunner extends PipelineRunner<PipelineResult> {
    public static NotRunnableRunner fromOptions(PipelineOptions opts) {
      return new NotRunnableRunner();
    }

    @Override
    public PipelineResult run(Pipeline pipeline) {
      throw new UnsupportedOperationException();
    }
  }
}
