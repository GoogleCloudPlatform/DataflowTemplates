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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Maps a Boundary Type from one to another. This allows the implementation to support splitting
 * types like strings which can be mapped to BigInteger.
 */
public interface BoundaryTypeMapper extends Serializable {
  /**
   * Map String to BigInteger.
   *
   * @param element String
   * @param lengthTOPad Length to pad the string, generally same as width of the column.
   * @param partitionColumn partition column details with {@link CollationReference} set.
   * @param c ProcessContext.
   * @return mapped BigInteger.
   */
  BigInteger mapStringToBigInteger(
      String element, int lengthTOPad, PartitionColumn partitionColumn, ProcessContext c);

  /**
   * Unmap BigInteger into String.
   *
   * @param element BigInteger
   * @param partitionColumn partition column details with {@link CollationReference} set.
   * @param c ProcessContext.
   * @return unmapped string.
   */
  String unMapStringFromBigInteger(
      BigInteger element, PartitionColumn partitionColumn, ProcessContext c);

  /**
   * Relay the pCollectionView for mapped Collations mapping ease of chaining transforms.
   *
   * @return pCollationView
   */
  PCollectionView<Map<CollationReference, CollationMapper>> getCollationMapperView();
}
