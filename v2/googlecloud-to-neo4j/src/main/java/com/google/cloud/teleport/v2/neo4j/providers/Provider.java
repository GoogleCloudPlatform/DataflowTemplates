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
package com.google.cloud.teleport.v2.neo4j.providers;

import com.google.cloud.teleport.v2.neo4j.model.helpers.SourceQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import java.util.List;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/** Provider interface, implemented for every source. */
public interface Provider {

  void configure(OptionsParams optionsParams, JobSpec jobSpecRequest);

  /**
   * Push down capability determine whether groupings and aggregations are executed as SQL queries.
   * When a source does not support push-down, we use SQLTransform in the beam engine. SQL transform
   * is horribly inefficient and does not support ordering.
   */
  boolean supportsSqlPushDown();

  /**
   * Returns validation errors as strings. This method implements provider specific validations.
   * Core engine validations are shared and executed separately.
   */
  List<String> validateJobSpec();

  /**
   * Queries the source if necessary. It will be necessary if there are no transforms or the source
   * does not support SQL push-down. For a SQL source with target transformations, this source query
   * will not be made.
   */
  PTransform<PBegin, PCollection<Row>> querySourceBeamRows(SourceQuerySpec sourceQuerySpec);

  /**
   * Queries the source for a particular target. The TargetQuerySpec includes the source query so
   * that sources that do not support push-down, additional transforms can be done in this
   * transform.
   */
  PTransform<PBegin, PCollection<Row>> queryTargetBeamRows(TargetQuerySpec targetQuerySpec);

  /**
   * Queries the source to extract metadata. This transform returns zero rows and a valid schema
   * specification.
   */
  PTransform<PBegin, PCollection<Row>> queryMetadata(Source source);
}
