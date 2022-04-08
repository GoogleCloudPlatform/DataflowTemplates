/*
 * Copyright (C) 2021 Google LLC
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

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A transform that can be used when a template detects that there is no useful work to perform.
 *
 * <p>Since the {@code pipeline.run()} is necessary to have in a template, and the run fails on a
 * pipeline without transforms, this transform can be applied to a pipeline to achieve the
 * do-nothing result.
 */
public class NoopTransform extends PTransform<PBegin, PDone> {
  private static final Logger LOG = LoggerFactory.getLogger(NoopTransform.class);

  @Override
  public PDone expand(PBegin input) {
    LOG.info("No items to process.");
    input.apply(Create.of(StringUtf8Coder.of()));
    return PDone.in(input.getPipeline());
  }
}
