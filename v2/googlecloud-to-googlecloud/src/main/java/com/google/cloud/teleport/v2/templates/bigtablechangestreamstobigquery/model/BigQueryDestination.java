package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model;

import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;

public class BigQueryDestination implements Serializable {

  private final String changelogTableName;
  private final boolean writeRowkeyAsBytes;
  private final boolean writeValueAsBytes;
  private final boolean writeNumericTimestamps;
  private final Set<String> changelogFieldsToIgnore;

  public BigQueryDestination(
      String changelogTableName,
      boolean writeRowkeyAsBytes,
      boolean writeValuesAsBytes,
      boolean writeNumericTimestamps,
      String bigQueryChangelogTablePartitionGranularity, // TODO: how to implement
      Long bigQueryChangelogTablePartitionExpirationMs,
      String bigQueryChangelogTableFieldsToIgnore
  ) {
    this.writeRowkeyAsBytes = writeRowkeyAsBytes;
    this.writeValueAsBytes = writeValuesAsBytes;
    this.writeNumericTimestamps = writeNumericTimestamps;
    this.changelogTableName = changelogTableName;
    if (StringUtils.isBlank(bigQueryChangelogTableFieldsToIgnore)) {
      this.changelogFieldsToIgnore = Collections.emptySet();
    } else {
      this.changelogFieldsToIgnore = Arrays.stream(
          bigQueryChangelogTableFieldsToIgnore.trim().split("[\\s]*,[\\s]*")
      ).map(s->s.toLowerCase(Locale.getDefault())).collect(Collectors.toSet());
    }
  }

  public String getChangelogTableName() {
    return changelogTableName;
  }

  public boolean isColumnEnabled(ChangelogColumn column) {
    switch (column) {
      case TIMESTAMP:
      case TIMESTAMP_FROM:
      case TIMESTAMP_TO:
        return !writeNumericTimestamps;
      case TIMESTAMP_NUM:
      case TIMESTAMP_FROM_NUM:
      case TIMESTAMP_TO_NUM:
        return writeNumericTimestamps;
      case ROW_KEY_STRING:
        return !writeRowkeyAsBytes;
      case ROW_KEY_BYTES:
        return writeRowkeyAsBytes;
      case VALUE_STRING:
        return !writeValueAsBytes;
      case VALUE_BYTES:
        return writeValueAsBytes;
      default:
        break;
    }

    return !column.isIgnorable() || !changelogFieldsToIgnore.contains(column.getBqColumnName());
  }

  public ImmutableSet<String> getIgnoredBigQueryColumnsNames() {
    Set<String> ignoredColumns = new HashSet<>();
    for (ChangelogColumn col: ChangelogColumn.values()) {
      if (col.isIgnorable() && changelogFieldsToIgnore.contains(col.getBqColumnName())) {
        ignoredColumns.add(col.getBqColumnName());
      }
    }
    return ImmutableSet.copyOf(ignoredColumns);
  }
}
