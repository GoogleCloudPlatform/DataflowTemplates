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
package com.google.cloud.teleport.templates.common;

import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class {@link CombineJsonLines}. */
public class CombineJsonLines extends PTransform<PCollection<String>, PCollection<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(CombineJsonLines.class);

  public PCollection<String> expand(PCollection<String> input) {
    return input
        .apply(WithKeys.of((Void) null))
        .apply(GroupByKey.create())
        .apply(Values.create())
        .apply(MapElements.into(TypeDescriptors.strings()).via(i -> String.join(",", i)));
  }

  public static CombineJsonLines create() {
    return new CombineJsonLines();
  }
}
