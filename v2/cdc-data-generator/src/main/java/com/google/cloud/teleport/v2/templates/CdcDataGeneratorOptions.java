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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.options.CommonTemplateOptions;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.StreamingOptions;

public interface CdcDataGeneratorOptions
    extends CommonTemplateOptions, GcpOptions, StreamingOptions {
  enum SinkType {
    SPANNER,
    MYSQL
  }

  /**
   * Baseline rate for the root tick pipeline, in ticks/second. {@code GenerateTicks} scales this
   * up or filters it down to match the schema's total root-table QPS. Exposed separately from
   * {@link #getInsertQps()} so operators can, for example, set a higher {@code baseTickRate}
   * than the sum of per-table QPS to avoid sub-tick probabilistic sampling on low-QPS schemas.
   * Nullable so {@code GenerateTicks}/{@code DataGenerator} can detect "unset" and fall through
   * to {@link #getInsertQps()} rather than silently defaulting to a hardcoded value that may
   * disagree with the declared per-table totals.
   */
  @Nullable
  Integer getBaseTickRate();

  void setBaseTickRate(Integer value);

  /**
   * Default insert QPS applied to each root table when the schema config doesn't specify one.
   * Also serves as the fallback for {@link #getBaseTickRate()} so that a user who sets only
   * {@code --insertQps} gets a coherent tick rate without needing to set both.
   */
  @Nullable
  Integer getInsertQps();

  void setInsertQps(Integer value);

  /**
   * Maximum number of synthetic logical shards to spread generated primary keys across when no
   * external shard file is provided. Shard ids are uniformly sampled from {@code
   * [0, maxShards)}, so setting this higher spreads traffic across more partitions downstream.
   */
  @Default.Integer(1)
  Integer getMaxShards();

  void setMaxShards(Integer value);

  /**
   * GCS path to a JSON file describing the sink configuration. For MySQL this file typically
   * points at a shard list; {@code GeneratePrimaryKey} reads the logical shard ids from here
   * and samples them instead of synthesising {@code shard<N>} placeholders.
   */
  @Nullable
  String getSinkOptionsPath();

  void setSinkOptionsPath(String value);
}
