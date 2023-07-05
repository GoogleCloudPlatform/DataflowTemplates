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
package com.google.cloud.teleport.v2.transforms;

import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.kinesis.KinesisRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link KinesisDataTransforms} extracts the string from Kinesis Records consumed by the
 * pipeline.
 */
public class KinesisDataTransforms {

  /** Transforms KinesisRecord received from AWS to string. */
  public static class ExtractStringFn extends DoFn<KinesisRecord, String> {
    private static final Logger LOG = LoggerFactory.getLogger(ExtractStringFn.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
      String content = new String(c.element().getDataAsBytes(), StandardCharsets.UTF_8);
      c.output(content);
    }
  }
}
