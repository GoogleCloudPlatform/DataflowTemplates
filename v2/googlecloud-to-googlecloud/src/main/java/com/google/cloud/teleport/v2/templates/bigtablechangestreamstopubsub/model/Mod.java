/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model;

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
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.commons.lang3.StringUtils;
import org.threeten.bp.Instant;

/**
 * The {@link Mod} contains the keys, new values (from {@link
 * com.google.cloud.bigtable.data.v2.models.Entry}) and metadata ({@link
 * com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation}) of a Bigtable changelog row.
 */
@DefaultCoder(AvroCoder.class)
public final class Mod implements Serializable {

  private static final long serialVersionUID = 8703757194338184299L;

  private String changeJson;
  private long commitTimestampSeconds;
  private int commitTimestampNanos;
  private ModType modType;

  private String _metadata_error;

  private Integer _metadata_retry_count;

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

    Map<String, Object> propertiesMap = Maps.newLinkedHashMap();
    setCommonProperties(propertiesMap, source, mutation);
    setSpecificProperties(propertiesMap, deleteFamily);
    this.changeJson = convertPropertiesToJson(propertiesMap);
  }

  private void setCommonProperties(
      Map<String, Object> propertiesMap, BigtableSource source, ChangeStreamMutation mutation) {
    propertiesMap.put(PubSubFields.ROW_KEY_BYTES.name(), encodeBytes(mutation.getRowKey()));
    propertiesMap.put(PubSubFields.SOURCE_INSTANCE.name(), source.getInstanceId());
    propertiesMap.put(PubSubFields.SOURCE_CLUSTER.name(), mutation.getSourceClusterId());
    propertiesMap.put(PubSubFields.SOURCE_TABLE.name(), source.getTableId());
    propertiesMap.put(PubSubFields.TIEBREAKER.name(), mutation.getTieBreaker());
    propertiesMap.put(
        PubSubFields.IS_GC.name(), mutation.getType() == MutationType.GARBAGE_COLLECTION);
    propertiesMap.put(
        PubSubFields.COMMIT_TIMESTAMP.name(),
        cbtTimestampToLongMicros(mutation.getCommitTimestamp()));
  }

  private Long cbtTimestampToLongMicros(Instant commitTimestamp) {
    long epochMicros = commitTimestamp.toEpochMilli() * 1000;
    long nanosAsMicros = commitTimestamp.getNano() / 1000;
    return epochMicros + nanosAsMicros;
  }

  private void setSpecificProperties(Map<String, Object> propertiesMap, SetCell setCell) {
    propertiesMap.put(PubSubFields.MOD_TYPE.name(), ModType.SET_CELL.getCode());
    propertiesMap.put(PubSubFields.COLUMN_FAMILY.name(), setCell.getFamilyName());
    propertiesMap.put(PubSubFields.COLUMN_BYTES.name(), encodeBytes(setCell.getQualifier()));
    propertiesMap.put(PubSubFields.TIMESTAMP.name(), setCell.getTimestamp());
    propertiesMap.put(PubSubFields.VALUE_BYTES.name(), encodeBytes(setCell.getValue()));
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

    propertiesMap.put(PubSubFields.MOD_TYPE.name(), ModType.DELETE_CELLS.getCode());
    propertiesMap.put(PubSubFields.COLUMN_FAMILY.name(), deleteCells.getFamilyName());
    propertiesMap.put(PubSubFields.COLUMN_BYTES.name(), encodeBytes(deleteCells.getQualifier()));
    propertiesMap.put(PubSubFields.TIMESTAMP_FROM.name(), startTimestamp);
    propertiesMap.put(PubSubFields.TIMESTAMP_TO.name(), endTimestamp);
  }

  private void setSpecificProperties(Map<String, Object> propertiesMap, DeleteFamily deleteFamily) {
    propertiesMap.put(PubSubFields.MOD_TYPE.name(), ModType.DELETE_FAMILY.getCode());
    propertiesMap.put(PubSubFields.COLUMN_FAMILY.name(), deleteFamily.getFamilyName());
  }

  public static Mod fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json, Mod.class);
  }

  /**
   * @return JSON object as String representing the changelog record
   */
  public String getChangeJson() {
    return changeJson;
  }

  /**
   * The seconds part of the timestamp at which the modifications within were committed in Cloud
   * Bigtable.
   */
  public long getCommitTimestampSeconds() {
    return commitTimestampSeconds;
  }

  /**
   * The nanoseconds part of the timestamp at which the modifications within were committed in Cloud
   * Bigtable.
   */
  public int getCommitTimestampNanos() {
    return commitTimestampNanos;
  }

  /** The type of operation that caused the modifications within this record. */
  public ModType getModType() {
    return modType;
  }

  public String get_metadata_error() {
    return _metadata_error;
  }

  public void set_metadata_error(String metadataError) {
    this._metadata_error = metadataError;
  }

  public Integer get_metadata_retry_count() {
    return _metadata_retry_count;
  }

  public void set_metadata_retry_count(Integer retryCount) {
    this._metadata_retry_count = retryCount;
  }

  @Override
  public boolean equals(@javax.annotation.Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Mod)) {
      return false;
    }
    Mod that = (Mod) o;
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

  private String cbtTimestampMicrosToPubSubInt(Long timestampMicros) {
    if (timestampMicros == null) {
      return null;
    }
    return Long.toString(timestampMicros);
  }

  private String convertPropertiesToJson(Map<String, Object> propertiesMap) {
    try {
      return new ObjectMapper().writeValueAsString(propertiesMap);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
