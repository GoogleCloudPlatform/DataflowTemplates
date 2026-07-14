/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.teleport.v2.templates.dofn.GeneratePrimaryKeyFn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * {@link PTransform} that generates synthetic primary-key {@link Row}s for each incoming {@link
 * DataGeneratorTable}.
 *
 * <p>See {@link GeneratePrimaryKeyFn} for the row-building details. This wrapper exists to:
 *
 * <ul>
 *   <li>Keep the {@code transforms/} package free of DoFn implementation details (mirrors the
 *       {@code SelectTable} / {@code SelectTableFn} split elsewhere in this module).
 *   <li>Pin the output coder. Row has no default coder so the output {@code PCollection} needs an
 *       explicit {@link KvCoder} that uses {@link SerializableCoder} for the Row half.
 * </ul>
 */
public class GeneratePrimaryKey
    extends PTransform<PCollection<DataGeneratorTable>, PCollection<KV<String, Row>>> {

  private final SinkConfig sinkConfig;
  private final String sinkType;

  private final String customJarPath;
  private final String customClassName;

  public GeneratePrimaryKey(
      SinkConfig sinkConfig, String sinkType, String customJarPath, String customClassName) {
    this.sinkConfig = sinkConfig;
    this.sinkType = sinkType;
    this.customJarPath = customJarPath;
    this.customClassName = customClassName;
  }

  public PCollection<KV<String, Row>> expand(PCollection<DataGeneratorTable> input) {
    return input
        .apply(
            "GeneratePrimaryKeyFn",
            ParDo.of(
                new GeneratePrimaryKeyFn(sinkConfig, sinkType, customJarPath, customClassName)))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Row.class)));
  }
}
