package com.google.cloud.teleport.v2.dto;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import java.io.Serializable;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class ComparisonRecord implements Serializable {

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
    Hasher hasher = Hashing.murmur3_128().newHasher();

    // 1. Compute hash by iterating all columns sorted by name
    SpannerHasherVisitor hasherVisitor = new SpannerHasherVisitor(hasher);
    spannerStruct.getType().getStructFields().stream()
        .filter(field -> !field.getName().equals("__tableName__"))
        .sorted(Comparator.comparing(Type.StructField::getName))
        .forEach(field -> SpannerValueVisitor.dispatch(
            spannerStruct.getValue(field.getName()), hasherVisitor));

    // Append table name to hash to ensure consistency with Avro
    SpannerValueVisitor.dispatch(spannerStruct.getValue("__tableName__"), hasherVisitor);
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

  public static ComparisonRecord fromAvroRecord(GenericRecord avroRecord) {
    Hasher hasher = Hashing.murmur3_128().newHasher();

    String tableName = avroRecord.get("tableName").toString();
    @SuppressWarnings("unchecked")
    List<String> primaryKeyNames = (List<String>) avroRecord.get("primaryKeys");
    // Ensure the list is safely cast/converted if Avro returns Utf8
    List<String> safePrimaryKeyNames = primaryKeyNames.stream()
        .map(Object::toString)
        .toList();

    GenericRecord payload = (GenericRecord) avroRecord.get("payload");
    org.apache.avro.Schema payloadSchema = payload.getSchema();

    // 1. Compute hash by iterating all fields in the payload schema sorted by name
    AvroHasherVisitor hasherVisitor = new AvroHasherVisitor(hasher);
    payloadSchema.getFields().stream()
        .sorted(Comparator.comparing(Field::name))
        .forEach(field -> {
          Object value = payload.get(field.pos());
          AvroValueVisitor.dispatch(value, field.schema(), hasherVisitor);
        });
    // Spanner query adds __tableName__ as the last column, so we must hash it too
    // to match.
    if (tableName != null) {
      hasherVisitor.visitString(tableName);
    }

    String hash = hasher.hash().toString();

    // 2. Extract primary key columns by looking them up by name in the payload
    AvroStringVisitor stringVisitor = new AvroStringVisitor();
    List<Column> primaryKeyColumns = safePrimaryKeyNames.stream()
        .map(pkName -> {
          // get(name) is valid for GenericRecord
          Object value = payload.get(pkName);
          // We need the schema for this field to dispatch safely
          Schema fieldSchema = payloadSchema.getField(pkName).schema();
          AvroValueVisitor.dispatch(value, fieldSchema, stringVisitor);
          return Column.builder()
              .setColName(pkName)
              .setColValue(stringVisitor.getResult())
              .build();
        })
        .collect(Collectors.toList());

    return builder()
        .setTableName(tableName)
        .setPrimaryKeyColumns(primaryKeyColumns)
        .setHash(hash)
        .build();
  }
}
