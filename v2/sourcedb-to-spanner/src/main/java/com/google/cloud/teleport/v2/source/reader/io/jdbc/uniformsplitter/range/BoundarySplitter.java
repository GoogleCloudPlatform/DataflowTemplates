/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range;

import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;

/**
 * Interface to split point for a boundary to split into half.
 *
 * <p>Note:
 *
 * <p>
 *
 * <ol>
 *   <li>Implementations must not assume that that start is less than end. It depends on the
 *       specific ordering used by the database schema.
 *   <li>Implementations must be overflow safe.
 *   <li>Implementations must guarantee that the splitter is idempotent. The same boundary must get
 *       same split point through the lifetime of the migration.
 *   <li>Implementations must guarantee that the database would teat the splitpoint as being
 *       in-between the start and end as per the ordering and collation used for the partition
 *       column.
 * </ol>
 */
public interface BoundarySplitter<T extends Serializable> extends Serializable {

  /**
   * Get Split point for start and end.
   *
   * @param start star of the boundary.
   * @param end end of the boundary.
   * @param partitionColumn column being split.
   * @param boundaryTypeMapper mapper for types like strings. Must not be null if strings are part
   *     of the partition column list.
   * @param processContext process context during transforms. Must not be null if strings are part
   *     of the partition column list.
   * @return
   */
  T getSplitPoint(
      T start,
      T end,
      @Nullable PartitionColumn partitionColumn,
      @Nullable BoundaryTypeMapper boundaryTypeMapper,
      @Nullable ProcessContext processContext);
}
