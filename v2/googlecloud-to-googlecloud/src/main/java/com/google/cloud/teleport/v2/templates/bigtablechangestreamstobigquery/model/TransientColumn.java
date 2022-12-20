package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.commons.lang3.Validate;

public enum TransientColumn {
  BQ_CHANGELOG_FIELD_NAME_ORIGINAL_PAYLOAD_JSON("_metadata_original_payload_json"),
  BQ_CHANGELOG_FIELD_NAME_ERROR("_metadata_error"),
  BQ_CHANGELOG_FIELD_NAME_RETRY_COUNT("_metadata_retry_count");

  private final String columnName;

  TransientColumn(String columnName) {
    this.columnName = columnName;
  }

  public String getColumnName() {
    return columnName;
  }

  public static boolean isTransientColumn(String columnName) {
    return COLUMN_NAMES.contains(columnName);
  }

  private final static Set<String> COLUMN_NAMES;
  static{
    COLUMN_NAMES = ImmutableSet.<String>builder()
        .add("_metadata_original_payload_json")
        .add("_metadata_error")
        .add("_metadata_retry_count")
        .build();

    for (TransientColumn column: TransientColumn.values()) {
      Validate.isTrue(COLUMN_NAMES.contains(column.getColumnName()));
    }
  }
}
