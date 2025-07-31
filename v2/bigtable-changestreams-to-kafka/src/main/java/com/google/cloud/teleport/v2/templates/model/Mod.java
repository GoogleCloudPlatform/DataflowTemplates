/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation.MutationType;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Serializable;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.commons.lang3.StringUtils;
import org.threeten.bp.Instant;

/**
 * The {@link Mod} contains the keys, new values (from {@link
 * com.google.cloud.bigtable.data.v2.models.Entry}) and metadata ({@link ChangeStreamMutation}) of a
 * Bigtable changelog row.
 */
@DefaultCoder(AvroCoder.class)
public final class Mod implements Serializable {

  private static final long serialVersionUID = 169227493747673831L;

  private String changeJson;
  private long commitTimestampSeconds;
  private int commitTimestampNanos;
  private ModType modType;

  // Constructor for serialization
  private Mod() {}

  private Mod(Instant commitTimestamp, ModType type) {
    this.commitTimestampNanos = commitTimestamp.getNano();
    this.commitTimestampSeconds = commitTimestamp.getEpochSecond();
    this.modType = type;
  }

  public Mod(BigtableSource source, ChangeStreamMutation mutation, SetCell setCell) {
    this(mutation.getCommitTimestamp(), ModType.SET_CELL);

    Map<String, Object> propertiesMap = Maps.newHashMap();
    setCommonProperties(propertiesMap, source, mutation);
    setSpecificProperties(propertiesMap, setCell);
    this.changeJson = convertPropertiesToJson(propertiesMap);
  }

  public Mod(BigtableSource source, ChangeStreamMutation mutation, DeleteCells deleteCells) {
    this.commitTimestampNanos = mutation.getCommitTimestamp().getNano();
    this.commitTimestampSeconds = mutation.getCommitTimestamp().getEpochSecond();
    this.modType = ModType.DELETE_CELLS;

    Map<String, Object> propertiesMap = Maps.newHashMap();
    setCommonProperties(propertiesMap, source, mutation);
    setSpecificProperties(propertiesMap, deleteCells);
    this.changeJson = convertPropertiesToJson(propertiesMap);
  }

  public Mod(BigtableSource source, ChangeStreamMutation mutation, DeleteFamily deleteFamily) {
    this.commitTimestampNanos = mutation.getCommitTimestamp().getNano();
    this.commitTimestampSeconds = mutation.getCommitTimestamp().getEpochSecond();
    this.modType = ModType.DELETE_FAMILY;

    Map<String, Object> propertiesMap = Maps.newHashMap();
    setCommonProperties(propertiesMap, source, mutation);
    setSpecificProperties(propertiesMap, deleteFamily);
    this.changeJson = convertPropertiesToJson(propertiesMap);
  }

  private void setCommonProperties(
      Map<String, Object> propertiesMap, BigtableSource source, ChangeStreamMutation mutation) {
    propertiesMap.put(KafkaFields.ROW_KEY.name(), encodeBytes(mutation.getRowKey()));
    propertiesMap.put(KafkaFields.SOURCE_INSTANCE.name(), source.getInstanceId());
    propertiesMap.put(KafkaFields.SOURCE_CLUSTER.name(), mutation.getSourceClusterId());
    propertiesMap.put(KafkaFields.SOURCE_TABLE.name(), source.getTableId());
    propertiesMap.put(KafkaFields.TIEBREAKER.name(), mutation.getTieBreaker());
    propertiesMap.put(
        KafkaFields.IS_GC.name(), mutation.getType() == MutationType.GARBAGE_COLLECTION);
    propertiesMap.put(
        KafkaFields.COMMIT_TIMESTAMP.name(),
        cbtTimestampToLongMicros(mutation.getCommitTimestamp()));
  }

  private Long cbtTimestampToLongMicros(Instant commitTimestamp) {
    long epochMicros = commitTimestamp.toEpochMilli() * 1000;
    long nanosAsMicros = commitTimestamp.getNano() / 1000;
    return epochMicros + nanosAsMicros;
  }

  private void setSpecificProperties(Map<String, Object> propertiesMap, SetCell setCell) {
    propertiesMap.put(KafkaFields.MOD_TYPE.name(), ModType.SET_CELL.name());
    propertiesMap.put(KafkaFields.COLUMN_FAMILY.name(), setCell.getFamilyName());
    propertiesMap.put(KafkaFields.COLUMN_BYTES.name(), encodeBytes(setCell.getQualifier()));
    propertiesMap.put(KafkaFields.TIMESTAMP.name(), setCell.getTimestamp());
    propertiesMap.put(KafkaFields.VALUE_BYTES.name(), encodeBytes(setCell.getValue()));
  }

  private void setSpecificProperties(Map<String, Object> propertiesMap, DeleteCells deleteCells) {
    Long startTimestamp = deleteCells.getTimestampRange().getStart();
    if (startTimestamp == null) {
      startTimestamp = 0L;
    }
    Long endTimestamp = deleteCells.getTimestampRange().getEnd();
    if (deleteCells.getTimestampRange().getEndBound() == BoundType.UNBOUNDED) {
      endTimestamp = null;
    }

    propertiesMap.put(KafkaFields.MOD_TYPE.name(), ModType.DELETE_CELLS.name());
    propertiesMap.put(KafkaFields.COLUMN_FAMILY.name(), deleteCells.getFamilyName());
    propertiesMap.put(KafkaFields.COLUMN_BYTES.name(), encodeBytes(deleteCells.getQualifier()));
    propertiesMap.put(KafkaFields.TIMESTAMP_FROM.name(), startTimestamp);
    propertiesMap.put(KafkaFields.TIMESTAMP_TO.name(), endTimestamp);
  }

  private void setSpecificProperties(Map<String, Object> propertiesMap, DeleteFamily deleteFamily) {
    propertiesMap.put(KafkaFields.MOD_TYPE.name(), ModType.DELETE_FAMILY.name());
    propertiesMap.put(KafkaFields.COLUMN_FAMILY.name(), deleteFamily.getFamilyName());
  }

  public static Mod fromJson(String json) throws IOException {
    RowJsonUtils.increaseDefaultStreamReadConstraints(100 * 1024 * 1024);
    return new ObjectMapper().readValue(json, Mod.class);
  }

  /**
   * @return JSON object as String representing the changelog record
   */
  public String getChangeJson() {
    return changeJson;
  }

  @Override
  public boolean equals(@javax.annotation.Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Mod that)) {
      return false;
    }
    return StringUtils.equals(changeJson, that.changeJson)
        && commitTimestampSeconds == that.commitTimestampSeconds
        && commitTimestampNanos == that.commitTimestampNanos
        && modType == that.modType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(changeJson, commitTimestampSeconds, commitTimestampNanos, modType);
  }

  @Override
  public String toString() {
    return "Mod{"
        + "changeJson='"
        + changeJson
        + '\''
        + ", commitTimestampSeconds="
        + commitTimestampSeconds
        + ", commitTimestampNanos="
        + commitTimestampNanos
        + ", modType="
        + modType
        + '}';
  }

  public String toJson() throws JsonProcessingException {
    return new ObjectMapper().writeValueAsString(this);
  }

  private String encodeBytes(ByteString rowKey) {
    if (rowKey == null) {
      return null;
    } else {
      return Base64.getEncoder().encodeToString(rowKey.toByteArray());
    }
  }

  private String convertPropertiesToJson(Map<String, Object> propertiesMap) {
    try {
      return new ObjectMapper().writeValueAsString(propertiesMap);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
