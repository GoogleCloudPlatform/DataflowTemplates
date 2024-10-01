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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundaryTypeMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps a Boundary Type from one to another. This allows the implementation to support splitting
 * types like strings which can be mapped to BigInteger. For Mapping strings, {@link
 * BoundaryTypeMapperImpl} gets the {@link CollationMapper} from the {@link
 * ProcessContext#sideInput(PCollectionView)}. While using {@link BoundaryTypeMapperImpl} it must be
 * ensured that the {@link CollationMapper} is available in the side input.
 */
@AutoValue
public abstract class BoundaryTypeMapperImpl implements BoundaryTypeMapper, Serializable {

  abstract PCollectionView<Map<CollationReference, CollationMapper>> collationMapperView();

  private static final Logger logger = LoggerFactory.getLogger(BoundaryTypeMapperImpl.class);

  /**
   * Map String to BigInteger.
   *
   * @param element String
   * @param lengthTOPad Length to pad the string, generally same as width of the column.
   * @param partitionColumn partition column details with {@link CollationReference} set.
   * @param c ProcessContext.
   * @return mapped BigInteger.
   * @throws RuntimeException if {@link CollationMapper} is not found in the process context.
   */
  @Override
  public BigInteger mapStringToBigInteger(
      String element, int lengthTOPad, PartitionColumn partitionColumn, ProcessContext c) {
    Map<CollationReference, CollationMapper> collationMap =
        (Map<CollationReference, CollationMapper>) c.sideInput(collationMapperView());
    if (!collationMap.containsKey(partitionColumn.stringCollation())) {
      logger.error(
          "No CollationMapper found for {}. Discovered collations are {}",
          partitionColumn,
          collationMap);
      throw new RuntimeException(
          String.format(
              "No CollationMapper found for %s. Discovered collations are %s",
              partitionColumn, collationMap));
    }
    CollationMapper mapper = collationMap.get(partitionColumn.stringCollation());
    return mapper.mapString(element, lengthTOPad);
  }

  /**
   * Unmap BigInteger into String.
   *
   * @param element BigInteger
   * @param partitionColumn partition column details with {@link CollationReference} set.
   * @param c ProcessContext.
   * @return unmapped string.
   * @throws RuntimeException if {@link CollationMapper} is not found in the process context.
   */
  @Override
  public String unMapStringFromBigInteger(
      BigInteger element, PartitionColumn partitionColumn, ProcessContext c) {
    Map<CollationReference, CollationMapper> collationMap =
        (Map<CollationReference, CollationMapper>) c.sideInput(collationMapperView());
    if (!collationMap.containsKey(partitionColumn.stringCollation())) {
      logger.error(
          "No CollationMapper found for {}. Discovered collations are {}",
          partitionColumn,
          collationMap);
      throw new RuntimeException(
          String.format(
              "No CollationMapper found for %s. Discovered collations are %s",
              partitionColumn, collationMap));
    }
    CollationMapper mapper = collationMap.get(partitionColumn.stringCollation());
    return mapper.unMapString(element);
  }

  /**
   * Relay the pCollectionView for mapped Collations mapping ease of chaining transforms.
   *
   * @return pCollationView
   */
  @Override
  public PCollectionView<Map<CollationReference, CollationMapper>> getCollationMapperView() {
    return collationMapperView();
  }

  public static Builder builder() {
    return new AutoValue_BoundaryTypeMapperImpl.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setCollationMapperView(
        PCollectionView<Map<CollationReference, CollationMapper>> value);

    public abstract BoundaryTypeMapperImpl build();
  }
}
