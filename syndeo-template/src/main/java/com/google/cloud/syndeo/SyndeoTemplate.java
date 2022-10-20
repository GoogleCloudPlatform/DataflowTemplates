/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.syndeo;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.syndeo.common.ProviderUtil;
import com.google.cloud.syndeo.common.ProviderUtil.TransformSpec;
import com.google.cloud.syndeo.v1.SyndeoV1.ConfiguredSchemaTransform;
import com.google.cloud.syndeo.v1.SyndeoV1.PipelineDescription;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.common.io.CharStreams;

public class SyndeoTemplate {

  public interface Options extends PipelineOptions {
    @Description("Pipeline Options.")
    @Validation.Required
    String getPipelineSpec();

    void setPipelineSpec(String gcsSpec);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    FileSystems.setDefaultPipelineOptions(options);
    PipelineDescription pipeline = readFromFile(options.getPipelineSpec());
    // Read proto as configuration.
    List<TransformSpec> specs = new ArrayList<>();
    for (ConfiguredSchemaTransform inst : pipeline.getTransformsList()) {
      specs.add(new TransformSpec(inst));
    }

    Pipeline p = Pipeline.create(options);
    // Run pipeline from configuration.
    ProviderUtil.applyConfigs(specs, PCollectionRowTuple.empty(p));
    p.run();
  }

  public static PipelineDescription readFromFile(String filename) {
    try {
      MatchResult result = FileSystems.match(filename);
      checkArgument(
          result.status() == Status.OK && !result.metadata().isEmpty(),
          "Failed to match any files with the pattern: " + filename);
      checkArgument(result.metadata().size() == 1, "Only expected one match!");
      ResourceId id = result.metadata().stream().findFirst().get().resourceId();
      Reader reader = Channels.newReader(FileSystems.open(id), StandardCharsets.ISO_8859_1.name());
      return PipelineDescription.parseFrom(
          ByteString.copyFrom(CharStreams.toString(reader), StandardCharsets.ISO_8859_1));
    } catch (IOException e) {
      throw new RuntimeException("Issue reading file.", e);
    }
  }
}
