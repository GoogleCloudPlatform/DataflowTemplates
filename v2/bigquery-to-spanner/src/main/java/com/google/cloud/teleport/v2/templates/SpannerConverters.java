package com.google.cloud.teleport.v2.templates;

import static java.util.stream.Collectors.toList;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.Op;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.schemas.LogicalTypes;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTimeFieldType;
import org.joda.time.Instant;
import org.joda.time.base.AbstractInstant;

/**
 * Converts from Beam Rows to Spanner mutations.
 */
public class SpannerConverters {

  public static final FieldType SQL_DATE_TYPE = FieldType
      .logicalType(new LogicalTypes.PassThroughLogicalType<Instant>(
          "SqlDateType", "", FieldType.DATETIME) {
      });

  public static PTransform<PCollection<Row>, PCollection<Mutation>> toMutation(String table,
      Op operation) {
    return new RowToMutation(table, operation);
  }

  /**
   * PTransform to convert Beam Rows Spanner to Mutations.
   */
  public static class RowToMutation extends
      PTransform<PCollection<Row>, PCollection<Mutation>> {


    private final Op operation;
    private final String table;

    private RowToMutation(String table, Op operation) {
      this.table = table;
      this.operation = operation;
    }

    @Override
    public PCollection<Mutation> expand(PCollection<Row> input) {
      return input.apply(ParDo.of(new RowToMutationDoFn(table, operation)));
    }

    /**
     * DoFn to convert a single Beam Row to a Spanner Mutation.
     */
    public static class RowToMutationDoFn extends DoFn<Row, Mutation> {

      private final String table;
      private final Mutation.Op operation;
      private transient LoadingCache<FieldType, SerializableFunction<?, Optional<Value>>> cache;

      public RowToMutationDoFn(String table, Op operation) {
        this.table = table;
        this.operation = operation;
      }

      static Mutation.WriteBuilder newMutationBuilder(String table, Op operation) {
        switch (operation) {
          case INSERT_OR_UPDATE:
            return Mutation.newInsertOrUpdateBuilder(table);
          case INSERT:
            return Mutation.newInsertBuilder(table);
          case UPDATE:
            return Mutation.newUpdateBuilder(table);
          case REPLACE:
            return Mutation.newReplaceBuilder(table);
          default:
            throw new UnsupportedOperationException(String
                .format("%s mutation is not supported by this pipeline", operation));
        }
      }

      private LoadingCache<FieldType, SerializableFunction<?, Optional<Value>>> getters() {
        if (cache == null) {
          cache = CacheBuilder
              .newBuilder()
              .build(CacheLoader.from(this::getter));
        }
        return cache;
      }

      @ProcessElement
      public void processElement(@Element Row row, OutputReceiver<Mutation> out) {
        out.output(rowToMutation(row));
      }

      private Mutation rowToMutation(Row row) {
        Mutation.WriteBuilder builder = newMutationBuilder(table, operation);
        for (Field field : row.getSchema().getFields()) {
          try {
            getters().get(field.getType()).apply(row.getValue(field.getName()))
                .ifPresent((value -> builder.set(field.getName()).to(value)));
          } catch (ExecutionException e) {
            throw new RuntimeException(e);
          }
        }
        return builder.build();
      }

      private <T> Struct rowToStruct(Row row) {
        Struct.Builder builder = Struct.newBuilder();
        for (Field field : row.getSchema().getFields()) {
          try {
            getters().get(field.getType()).apply(row.getValue(field.getName()))
                .ifPresent((value -> builder.set(field.getName()).to(value)));
          } catch (ExecutionException e) {
            e.printStackTrace();
          }
        }
        return builder.build();
      }


      private <T> SerializableFunction<T, Optional<Value>> getter(FieldType type) {
        return optional(getValueGetter(type));
      }

      private <InT, OutT> SerializableFunction<InT, Optional<OutT>> optional(
          SerializableFunction<InT, OutT> func) {
        return (InT a) -> Optional.ofNullable(a).map(func::apply);
      }

      @VisibleForTesting
      protected <T> SerializableFunction<T, Value> getValueGetter(FieldType type) {
        switch (type.getTypeName()) {
          case BOOLEAN:
            return (T o) -> Value.bool((Boolean) o);
          case BYTES:
            return (T o) -> Value.bytes(ByteArray.copyFrom((byte[]) o));
          case DATETIME:
            return (T o) -> Value.timestamp(
                com.google.cloud.Timestamp
                    .ofTimeMicroseconds(((AbstractInstant) o).getMillis() * 1000));
          case DOUBLE:
            return (T o) -> Value.float64((Double) o);
          case FLOAT:
            return (T o) -> Value.float64((Float) o);
          case INT16:
            return (T o) -> Value.int64((Short) o);
          case INT32:
            return (T o) -> Value.int64((Integer) o);
          case INT64:
            return (T o) -> Value.int64((Long) o);
          case STRING:
            return (T o) -> Value.string(o.toString());
          case ARRAY:
            return (T o) -> getArrayValueGetter(type).apply((List<?>) o);
          case ROW:
            /*
             * Note, while we implement creating Struct objects from Rows, this will fail when
             * writing as a mutation, because Spanner does not support struct column types.
             */
            return (T o) -> Value.struct(rowToStruct((Row) o));
          case LOGICAL_TYPE:
            LogicalType logicalType = Objects.requireNonNull(type.getLogicalType());
            if (type.typesEqual(SQL_DATE_TYPE)) {
              return (T o) -> {
                AbstractInstant instant = (AbstractInstant) o;
                return Value.date(
                    Date.fromYearMonthDay(
                        instant.get(DateTimeFieldType.year()),
                        instant.get(DateTimeFieldType.monthOfYear()),
                        instant.get(DateTimeFieldType.dayOfMonth())));
              };
            }
            throw new UnsupportedOperationException(
                String.format(
                    "Converting logical type %s for spanner mutation not implemented, baseType=%s",
                    logicalType.getIdentifier(), logicalType.getBaseType().getTypeName()));

          case BYTE:
          case MAP:
          case DECIMAL:
          default:
            throw new UnsupportedOperationException(
                String.format("Converting type %s for spanner mutation not implemented",
                    type.getTypeName()));
        }
      }

      private SerializableFunction<List<?>, Value> getArrayValueGetter(FieldType type) {
        Preconditions.checkNotNull(type.getCollectionElementType());
        switch (type.getCollectionElementType().getTypeName()) {
          case BOOLEAN:
            return (List<?> lst) -> Value
                .boolArray(lst.stream().map(o -> (Boolean) o).collect(toList()));
          case BYTES:
            return (List<?> lst) -> Value.bytesArray(
                lst.stream().map(o -> ByteArray.copyFrom((byte[]) o)).collect(toList()));
          case DATETIME:
            return (List<?> lst) -> Value.timestampArray(
                lst.stream().map(o -> com.google.cloud.Timestamp
                    .ofTimeMicroseconds(((AbstractInstant) o).getMillis() * 1000))
                    .collect(toList()));
          case FLOAT:
            return (List<?> lst) -> Value
                .float64Array(lst.stream().map(o -> Double.valueOf((Float) o)).collect(toList()));
          case DOUBLE:
            return (List<?> lst) -> Value
                .float64Array(lst.stream().map(o -> (Double) o).collect(toList()));
          case INT16:
            return (List<?> lst) -> Value
                .int64Array(lst.stream().map(o -> Long.valueOf((Short) o)).collect(toList()));
          case INT32:
            return (List<?> lst) -> Value
                .int64Array(lst.stream().map(o -> Long.valueOf((Integer) o)).collect(toList()));
          case INT64:
            return (List<?> lst) -> Value
                .int64Array(lst.stream().map(o -> (Long) o).collect(toList()));
          case STRING:
            return (List<?> lst) -> Value
                .stringArray(lst.stream().map(Object::toString).collect(toList()));
          case ARRAY:
            throw new UnsupportedOperationException(
                "Cannot construct array with element type ARRAY<?> because nested arrays are not supported by Spanner");
          case BYTE:
          case LOGICAL_TYPE:
          case MAP:
          case ROW:
          case DECIMAL:
          default:
            throw new UnsupportedOperationException(
                String
                    .format("Converting array element type %s for spanner mutation not implemented",
                        type.getTypeName()));
        }
      }
    }
  }
}
