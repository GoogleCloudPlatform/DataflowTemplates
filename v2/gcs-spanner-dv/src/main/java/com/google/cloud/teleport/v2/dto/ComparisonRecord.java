package com.google.cloud.teleport.v2.dto;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Struct;
import com.google.common.hash.Hashing;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

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
    com.google.common.hash.Hasher hasher = Hashing.murmur3_128().newHasher();

    // 1. Compute hash by iterating all columns by index
    SpannerHasherVisitor hasherVisitor = new SpannerHasherVisitor(hasher);
    for (int i = 0; i < spannerStruct.getColumnCount(); i++) {
      SpannerValueVisitor.dispatch(spannerStruct.getValue(i), hasherVisitor);
    }
    String hash = hasher.hash().toString();

    // 2. Extract primary key columns by looking them up by name
    SpannerStringVisitor stringVisitor = new SpannerStringVisitor();
    List<Column> primaryKeyColumns = primaryKeyColumnNames.stream()
        .map(
            pkName -> {
              SpannerValueVisitor.dispatch(spannerStruct.getValue(pkName), stringVisitor);
              return Column.builder()
                  .setColName(pkName)
                  .setColValue(stringVisitor.getResult())
                  .build();
            })
        .collect(Collectors.toList());

    return builder()
        .setTableName(spannerStruct.getString("__tableName__"))
        .setPrimaryKeyColumns(primaryKeyColumns)
        .setHash(hash)
        .build();
  }
}
