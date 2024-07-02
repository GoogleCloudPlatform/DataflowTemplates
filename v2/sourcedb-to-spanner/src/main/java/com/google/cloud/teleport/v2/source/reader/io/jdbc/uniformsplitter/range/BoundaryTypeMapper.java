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
import java.math.BigInteger;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;

/**
 * Maps a Boundary Type from one to another. This allows the implementation to support splitting
 * types like strings which can be mapped to BigInteger.
 */
public interface BoundaryTypeMapper extends Serializable {
  BigInteger mapString(
      String element, int lengthTOPad, PartitionColumn partitionColumn, ProcessContext c);

  String unMapString(BigInteger element, PartitionColumn partitionColumn, ProcessContext c);
}
