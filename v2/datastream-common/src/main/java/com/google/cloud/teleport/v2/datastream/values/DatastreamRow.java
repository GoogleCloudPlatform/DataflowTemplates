/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.datastream.values;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DatastreamRow} class holds the value of a specific Datastream JSON record. The data
 * represents 1 CDC event from a given source. The class is intended to contain all logic used when
 * cleaning or extracting Datastream data details.
 */
// @DefaultCoder(FailsafeElementCoder.class)
public class DatastreamRow {

  public static final String DEFAULT_ORACLE_PRIMARY_KEY = "_metadata_row_id";
  public static final String ORACLE_TRANSACTION_ID_KEY = "_metadata_tx_id";

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamRow.class);
  private TableRow tableRow;
  private JsonNode jsonRow;

  private DatastreamRow(TableRow tableRow, JsonNode jsonRow) {
    this.tableRow = tableRow;
    this.jsonRow = jsonRow;
  }

  /**
   * Build a {@code DatastreamRow} for use in pipelines.
   *
   * @param tableRow A TableRow object with Datastream data in it.
   */
  public static DatastreamRow of(TableRow tableRow) {
    return new DatastreamRow(tableRow, null);
  }

  /**
   * Build a {@code DatastreamRow} for use in pipelines.
   *
   * @param jsonRow A JsonNode object with Datastream data in it.
   */
  public static DatastreamRow of(JsonNode jsonRow) {
    return new DatastreamRow(null, jsonRow);
  }

  /* Return the String stream name for the given data. */
  public String getStreamName() {
    return getStringValue("_metadata_stream");
  }

  /* Return the String source type for the given data (eg. mysql, oracle). */
  public String getSourceType() {
    return getStringValue("_metadata_source_type");
  }

  /* Return the String source schema name for the given data. */
  public String getSchemaName() {
    return getStringValue("_metadata_schema");
  }

  /* Return the String source table name for the given data. */
  public String getTableName() {
    return getStringValue("_metadata_table");
  }

  public String getStringValue(String field) {
    if (this.jsonRow != null) {
      JsonNode node = jsonRow.get(field);
      return (node != null && !node.isNull()) ? node.textValue() : null;
    } else {
      return (tableRow != null && tableRow.get(field) != null)
          ? (String) tableRow.get(field)
          : null;
    }
  }

  private Object getFieldValue(String field) {
    if (this.jsonRow != null) {
      return jsonRow.get(field);
    } else {
      return tableRow != null ? tableRow.get(field) : null;
    }
  }

  /* Returns the list of primary keys for the given row from the Datastream data. */
  public List<String> getPrimaryKeys() {
    List<String> primaryKeys = new ArrayList<>();
    if (this.jsonRow != null) {
      JsonNode pkNode = jsonRow.get("_metadata_primary_keys");
      if (pkNode != null && !pkNode.isNull() && pkNode.isArray()) {
        for (JsonNode node : pkNode) {
          primaryKeys.add(node.asText());
        }
      }
      if (primaryKeys.isEmpty()) {
        // Fallback to source metadata (e.g. for SQL Server CDC where primary_keys may be in
        // replication_index)
        JsonNode sourceMetadata = jsonRow.get("_metadata_source");
        if (sourceMetadata == null || sourceMetadata.isNull()) {
          sourceMetadata = jsonRow.get("source_metadata");
        }
        if (sourceMetadata != null && !sourceMetadata.isNull()) {
          JsonNode srcPks = sourceMetadata.get("primary_keys");
          if (srcPks != null && !srcPks.isNull() && srcPks.isArray()) {
            for (JsonNode node : srcPks) {
              primaryKeys.add(node.asText());
            }
          }
          if (primaryKeys.isEmpty()) {
            JsonNode replIndex = sourceMetadata.get("replication_index");
            if (replIndex != null && !replIndex.isNull() && replIndex.isArray()) {
              for (JsonNode node : replIndex) {
                primaryKeys.add(node.asText());
              }
            }
          }
        }
      }
    } else if (this.tableRow != null) {
      if (tableRow.get("_metadata_primary_keys") != null) {
        Object primaryKeysObj = tableRow.get("_metadata_primary_keys");
        if (primaryKeysObj instanceof List) {
          primaryKeys = (List<String>) primaryKeysObj;
        } else if (primaryKeysObj instanceof String) {
          // Fixed since we have seen this once.
          // the reason isn't clear as DS template always set a List on this field.
          // we saw this happens  on "UDF to TableRow/Oracle Cleaner" or "BigQuery Merge/Build"
          // steps.
          LOG.info("primaryKeysObj is String type {}", primaryKeysObj);
          String primaryKeysStr = (String) primaryKeysObj;
          if (primaryKeysStr.startsWith("[") && primaryKeysStr.endsWith("]")) {
            String[] elements = primaryKeysStr.substring(1, primaryKeysStr.length() - 1).split(",");
            primaryKeys =
                new ArrayList<>(
                    Arrays.asList(elements).stream()
                        .map(s -> StringUtils.unwrap(s.trim(), "\""))
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toList()));
          }
        } else {
          throw new RuntimeException(
              "_metadata_primary_keys has unsupported type: " + primaryKeysObj.getClass());
        }
      }
      if (primaryKeys.isEmpty() && tableRow.get("replication_index") != null) {
        Object replObj = tableRow.get("replication_index");
        if (replObj instanceof List) {
          primaryKeys = (List<String>) replObj;
        }
      }
      if (primaryKeys.isEmpty() && tableRow.get("_metadata_source") instanceof Map) {
        Map<String, Object> srcMeta = (Map<String, Object>) tableRow.get("_metadata_source");
        if (srcMeta.get("replication_index") instanceof List) {
          primaryKeys = (List<String>) srcMeta.get("replication_index");
        } else if (srcMeta.get("primary_keys") instanceof List) {
          primaryKeys = (List<String>) srcMeta.get("primary_keys");
        }
      } else if (primaryKeys.isEmpty() && tableRow.get("source_metadata") instanceof Map) {
        Map<String, Object> srcMeta = (Map<String, Object>) tableRow.get("source_metadata");
        if (srcMeta.get("replication_index") instanceof List) {
          primaryKeys = (List<String>) srcMeta.get("replication_index");
        } else if (srcMeta.get("primary_keys") instanceof List) {
          primaryKeys = (List<String>) srcMeta.get("primary_keys");
        }
      }
    }

    if ("oracle".equals(this.getSourceType()) && primaryKeys.isEmpty()) {
      primaryKeys.add(DEFAULT_ORACLE_PRIMARY_KEY);
    }

    return primaryKeys;
  }

  /* Returns the formatted string after applying the data inside the row. */
  public String formatStringTemplate(String template) {
    // Key/Value Map used to replace values in template
    Map<String, String> values = new HashMap<>();

    for (String fieldName : getFieldNames()) {
      Object value = getFieldValue(fieldName);
      if (value instanceof String) {
        values.put(fieldName, (String) value);
      }
    }

    // Substitute any templated values in the template
    String result = StringSubstitutor.replace(template, values, "{", "}");
    return result;
  }

  /* Returns the formatted string after applying the data inside the row and stripped invalid BigQuery characters. */
  public String formatStringTemplateForBigQuery(String template) {
    return BigQueryConverters.sanitizeBigQueryChars(this.formatStringTemplate(template), "_");
  }

  /**
   * Returns the formatted string after applying the data inside the row and stripped invalid
   * BigQuery Dataset characters.
   */
  public String formatStringTemplateForBigQueryDataset(String template) {
    return BigQueryConverters.sanitizeBigQueryDatasetChars(
        this.formatStringTemplate(template), "_");
  }

  /* Returns the list of field/column names for the given row. */
  public Iterable<String> getFieldNames() {
    if (this.jsonRow != null) {
      return ImmutableList.copyOf(jsonRow.fieldNames());
    } else {
      return tableRow.keySet();
    }
  }

  public boolean isDeleted() {
    if (this.jsonRow != null) {
      JsonNode deletedNode = jsonRow.get("_metadata_deleted");
      if (deletedNode != null && !deletedNode.isNull()) {
        return deletedNode.asBoolean();
      }
      JsonNode changeTypeNode = jsonRow.get("_metadata_change_type");
      if (changeTypeNode != null && !changeTypeNode.isNull()) {
        return "DELETE".equalsIgnoreCase(changeTypeNode.asText());
      }
      JsonNode sourceMetadata = jsonRow.get("_metadata_source");
      if (sourceMetadata == null || sourceMetadata.isNull()) {
        sourceMetadata = jsonRow.get("source_metadata");
      }
      if (sourceMetadata != null && !sourceMetadata.isNull()) {
        JsonNode srcDeleted = sourceMetadata.get("is_deleted");
        if (srcDeleted != null && !srcDeleted.isNull()) {
          return srcDeleted.asBoolean();
        }
        JsonNode srcChangeType = sourceMetadata.get("change_type");
        if (srcChangeType != null && !srcChangeType.isNull()) {
          return "DELETE".equalsIgnoreCase(srcChangeType.asText());
        }
      }
      return false;
    } else if (this.tableRow != null) {
      Object deleted = tableRow.get("_metadata_deleted");
      if (deleted instanceof Boolean) {
        return (Boolean) deleted;
      } else if (deleted instanceof String) {
        return Boolean.parseBoolean((String) deleted);
      }
      Object changeType = tableRow.get("_metadata_change_type");
      if (changeType instanceof String) {
        return "DELETE".equalsIgnoreCase((String) changeType);
      }
      Map<String, Object> srcMeta = null;
      if (tableRow.get("_metadata_source") instanceof Map) {
        srcMeta = (Map<String, Object>) tableRow.get("_metadata_source");
      } else if (tableRow.get("source_metadata") instanceof Map) {
        srcMeta = (Map<String, Object>) tableRow.get("source_metadata");
      }
      if (srcMeta != null) {
        if (srcMeta.get("is_deleted") instanceof Boolean) {
          return (Boolean) srcMeta.get("is_deleted");
        }
        if (srcMeta.get("change_type") instanceof String) {
          return "DELETE".equalsIgnoreCase((String) srcMeta.get("change_type"));
        }
      }
      return false;
    }
    return false;
  }

  public List<String> getSortFields(Boolean addIsDeleted) {
    List<String> sortFields = new ArrayList<>(getSortFields());
    if (addIsDeleted) {
      sortFields.add("_metadata_deleted");
    }
    return sortFields;
  }

  public List<String> getSortFields() {
    String sourceType = this.getSourceType();
    if ("mysql".equalsIgnoreCase(sourceType)) {
      return Arrays.asList("_metadata_timestamp", "_metadata_log_file", "_metadata_log_position");
    } else if ("postgresql".equalsIgnoreCase(sourceType)
        || "postgres".equalsIgnoreCase(sourceType)) {
      return Arrays.asList("_metadata_timestamp", "_metadata_lsn");
    } else if ("sqlserver".equalsIgnoreCase(sourceType)) {
      return Arrays.asList("_metadata_timestamp", "_metadata_lsn");
    } else {
      // Current default is oracle.
      return Arrays.asList(
          "_metadata_timestamp", "_metadata_scn", "_metadata_rs_id", "_metadata_ssn");
    }
  }

  @Override
  public String toString() {
    if (this.jsonRow != null) {
      return this.jsonRow.toString();
    } else {
      return this.tableRow.toString();
    }
  }

  public String getOracleRowId() {
    return this.getStringValue(DEFAULT_ORACLE_PRIMARY_KEY);
  }

  public String getOracleTxnId() {
    return this.getStringValue(ORACLE_TRANSACTION_ID_KEY);
  }
}
