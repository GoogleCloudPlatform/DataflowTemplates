package com.google.cloud.teleport.v2.dto;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.commons.codec.binary.Base64;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class ComparisonRecord {

  public abstract String getTableName();

  public abstract List<Column> getPrimaryKeyColumns();

  public abstract String getHash();

  public static Builder builder() {
    return new AutoValue_ComparisonRecord.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setTableName(String tableName);

    public abstract Builder setPrimaryKeyColumns(List<Column> primaryKeyColumns);

    public abstract Builder setHash(String hash);

    public abstract ComparisonRecord build();
  }

  public static ComparisonRecord fromSpannerStruct(Struct spannerStruct,
      List<String> primaryKeyColumnNames) {
    int nCols = spannerStruct.getColumnCount();
    StringBuilder sbConcatCols = new StringBuilder();

    // 1. Compute hash by iterating all columns by index
    for (int i = 0; i < nCols; i++) {
      sbConcatCols.append(valueToString(spannerStruct.getValue(i)));
    }
    String hash = Hashing.murmur3_128()
        .hashString(sbConcatCols.toString(), StandardCharsets.UTF_8).toString();

    // 2. Extract primary key columns by looking them up by name
    List<Column> primaryKeyColumns = primaryKeyColumnNames.stream()
        .map(
            pkName -> Column.builder()
                .setColName(pkName)
                .setColValue(valueToString(spannerStruct.getValue(pkName)))
                .build())
        .collect(Collectors.toList());

    return builder()
        .setTableName(spannerStruct.getString("__tableName__"))
        .setPrimaryKeyColumns(primaryKeyColumns)
        .setHash(hash)
        .build();
  }

  private static String valueToString(Value value) {
    if (value.isNull()) {
      return "";
    }
    return switch (value.getType().getCode()) {
      case STRING -> value.getString();
      case BYTES -> Base64.encodeBase64String(value.getBytes().toByteArray());
      case INT64 -> String.valueOf(value.getInt64());
      case FLOAT64 -> String.valueOf(value.getFloat64());
      case NUMERIC -> String.valueOf(value.getNumeric());
      case DATE -> {
        com.google.cloud.Date date = value.getDate();
        yield String.format("%d%d%d",
            date.getYear(),
            date.getMonth(),
            date.getDayOfMonth());
      }
      case BOOL -> String.valueOf(value.getBool());
      default -> throw new RuntimeException(String.format("Unsupported type: %s", value.getType()));
    };
  }
}
