/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.google.cloud.teleport.bigtable;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.ReadableInstant;

/**
 * This DoFn takes a Beam {@link Row} as input and converts to Bigtable {@link Mutation}s. Primitive
 * Cell values are serialized using the Hbase {@link Bytes} class. Complex types are expanded into
 * multiple cells.
 *
 * <p>Collections: A list or set named 'mylist' with three values will expand into three cell
 * values; 'mylist[0]', 'mylist[1]', 'mylist[2]'.
 *
 * <p>Maps: Maps follow a similar pattern but have .key or .value appended as a suffix to
 * distinguish the keys and values. As such a map called 'mymap' with two key value pairs expand
 * into four cell values: 'mymap[0].key', 'mymap[0].value', 'mymap[1].key', 'mymap[1].value'
 *
 * <p>Row values with comments starting with 'rowkeyorder' indicate that they should be part of the
 * rowkey. These fields do not get serialized as cell values but are instead joined to form the
 * Bigtable Rowkey.
 *
 * <p>The Bigtable rowkey is created by joining all row-key cells serialized as strings in order
 * with a separator specified in the constructor.
 *
 * @see Bytes
 * @see <a
 *     href="https://www.datastax.com/dev/blog/the-most-important-thing-to-know-in-cassandra-data-modeling-the-primary-key">
 *     The Cassandra Row Key </a>
 */
public class BeamRowToBigtableFn extends DoFn<Row, KV<ByteString, Iterable<Mutation>>> {

  private static final String ROW_KEY_DESCRIPTION_PREFIX = "rowkeyorder";
  private final ValueProvider<String> defaultColumnFamily;
  private final ValueProvider<String> defaultRowKeySeparator;

  /**
   * @param defaultRowKeySeparator the row key field separator. See above for details.
   * @param defaultColumnFamily the default column family to write cell values & column qualifiers.
   */
  BeamRowToBigtableFn(
      ValueProvider<String> defaultRowKeySeparator, ValueProvider<String> defaultColumnFamily) {
    this.defaultColumnFamily = defaultColumnFamily;
    this.defaultRowKeySeparator = defaultRowKeySeparator;
  }

  @ProcessElement
  @SuppressWarnings("unused")
  public void processElement(
      @Element Row row, OutputReceiver<KV<ByteString, Iterable<Mutation>>> out) {

    // Generate the Bigtable Rowkey. This key will be used for all Cells for this row.
    ByteString rowkey = generateRowKey(row);

    // DoFn return value.
    ImmutableList.Builder<Mutation> mutations = ImmutableList.builder();

    // Retrieve all fields that are not part of the rowkey. All fields that are part of the
    // rowkey must have a metadata key set to key_order. All other fields should become their own
    // cells in Bigtable.
    List<Field> nonKeyColumns =
        row.getSchema().getFields().stream()
            .filter(
                f ->
                    f.getType()
                        .getMetadataString(CassandraRowMapperFn.KEY_ORDER_METADATA_KEY)
                        .isEmpty())
            .collect(Collectors.toList());

    // Iterate over all the fields with three cases. Primitives, collections and maps.
    for (Field field : nonKeyColumns) {
      TypeName type = field.getType().getTypeName();
      if (type.isPrimitiveType()) {
        ByteString value = primitiveFieldToBytes(type, row.getValue(field.getName()));
        SetCell cell = createCell(defaultColumnFamily.get(), field.getName(), value);
        mutations.add(Mutation.newBuilder().setSetCell(cell).build());
      } else if (type.isCollectionType()) {
        List<Mutation> collectionMutations = createCollectionMutations(row, field);
        mutations.addAll(collectionMutations);
      } else if (type.isMapType()) {
        List<Mutation> mapMutations = createMapMutations(row, field);
        mutations.addAll(mapMutations);
      } else {
        throw new UnsupportedOperationException(
            "Mapper does not support type:" + field.getType().getTypeName().toString());
      }
    }
    out.output(KV.of(rowkey, mutations.build()));
  }

  /**
   * Method generates a set of Bigtable mutations from the field specified in the input. Example: a
   * collection named ‘mycolumn’ with three values would be expanded into three column qualifiers in
   * inside Cloud Bigtable called, ‘mycolumn[0]’, ‘mycolumn[1]’ ,‘mycolumn[2]’. Cell values will be
   * serialized with the {@link BeamRowToBigtableFn#primitiveFieldToBytes(TypeName, Object)}
   * primitiveFieldToBytes} method.
   *
   * @param row The row to get the collection from.
   * @param field The field pointing to a collection in the row.
   * @return A set of mutation on the format specified above.
   */
  private List<Mutation> createCollectionMutations(Row row, Field field) {
    List<Mutation> mutations = new ArrayList<>();
    List<Object> list = new ArrayList<Object>(row.getArray(field.getName()));
    TypeName collectionElementType = field.getType().getCollectionElementType().getTypeName();
    for (int i = 0; i < list.size(); i++) {
      String fieldName = field.getName() + "[" + i + "]";
      ByteString value = primitiveFieldToBytes(collectionElementType, list.get(i));
      SetCell cell = createCell(defaultColumnFamily.get(), fieldName, value);
      mutations.add(Mutation.newBuilder().setSetCell(cell).build());
    }
    return mutations;
  }

  /**
   * Maps are serialized similarly to {@link BeamRowToBigtableFn#createMapMutations(Row, Field)}
   * collections} however with an additional suffix to denote if the cell is a key or a value. As
   * such a map column in Cassandra named ‘mycolumn’ with two key-value pairs would be expanded into
   * the following column qualifiers: ‘mycolumn[0].key’, ‘mycolumn[1].key’, ‘mycolumn[0].value’,
   * ‘mycolumn[1].value’. Cell values will be serialized with the {@link
   * BeamRowToBigtableFn#primitiveFieldToBytes(TypeName, Object)} primitiveFieldToBytes} method.
   *
   * @param row The row to get the collection from.
   * @param field The field pointing to a collection in the row.
   * @return A set of mutation on the format specified above.
   */
  private List<Mutation> createMapMutations(Row row, Field field) {
    List<Mutation> mutations = new ArrayList<>();
    // We use tree-map here to make sure that the keys are sorted.
    // Otherwise we get unpredictable serialization order of the keys in the mutation.
    Map<Object, Object> map = new TreeMap(row.getMap(field.getName()));
    TypeName mapKeyType = field.getType().getMapKeyType().getTypeName();
    TypeName mapValueType = field.getType().getMapValueType().getTypeName();
    Set<Map.Entry<Object, Object>> entries = map.entrySet();
    Iterator<Map.Entry<Object, Object>> iterator = entries.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      Map.Entry<Object, Object> entry = iterator.next();
      // Key
      String keyFieldName = field.getName() + "[" + i + "].key";
      ByteString keyValue = primitiveFieldToBytes(mapKeyType, entry.getKey());
      SetCell keyCell = createCell(defaultColumnFamily.get(), keyFieldName, keyValue);
      mutations.add(Mutation.newBuilder().setSetCell(keyCell).build());

      // Value
      String valueFieldName = field.getName() + "[" + i + "].value";
      ByteString valueValue = primitiveFieldToBytes(mapValueType, entry.getValue());
      SetCell valueCell = createCell(defaultColumnFamily.get(), valueFieldName, valueValue);
      mutations.add(Mutation.newBuilder().setSetCell(valueCell).build());

      i++;
    }
    return mutations;
  }

  /**
   * Helper method to create a SetCell operation.
   * @param columnFamily the column family to apply the value on.
   * @param columnQualifier the column qualifier to apply the value on.
   * @param value the value to apply.
   * @return
   */
  private SetCell createCell(String columnFamily, String columnQualifier, ByteString value) {
    return SetCell.newBuilder()
        .setFamilyName(columnFamily)
        .setColumnQualifier(ByteString.copyFrom(columnQualifier, Charset.defaultCharset()))
        .setValue(value)
        .build();
  }

  /**
   * Method serializes a primitive to a ByteString. Most types are serialized with the {@link Bytes toBytes toBytes} method.
   * Bytes and byte arrays pass through as they are whilst DATETIME types gets converted to a ISO8601 formatted String.
   * @param type the {@link Row row} field type
   * @param value the value from the {@link Row row}
   * @return a ByteString.
   */
  private ByteString primitiveFieldToBytes(TypeName type, Object value) {

    if (value == null) {
      return ByteString.EMPTY;
    }

    byte[] bytes;

    switch (type) {
      case BYTE:
        bytes = new byte[1];
        bytes[0] = (Byte) value;
        break;
      case INT16:
        bytes = Bytes.toBytes((Short) value);
        break;
      case INT32:
        bytes = Bytes.toBytes((Integer) value);
        break;
      case INT64:
        bytes = Bytes.toBytes((Long) value);
        break;
      case DECIMAL:
        bytes = Bytes.toBytes((BigDecimal) value);
        break;
      case FLOAT:
        bytes = Bytes.toBytes((Float) value);
        break;
      case DOUBLE:
        bytes = Bytes.toBytes((Double) value);
        break;
      case STRING:
        bytes = Bytes.toBytes((String) value);
        break;
      case DATETIME:
        ReadableInstant dateTime = ((ReadableInstant) value);
        // Write a ISO8601 formatted String.
        bytes = Bytes.toBytes(dateTime.toString());
        break;
      case BOOLEAN:
        bytes = Bytes.toBytes((Boolean) value);
        break;
      case BYTES:
        bytes = (byte[]) value;
        break;
      default:
        throw new UnsupportedOperationException("This method only supports primitives.");
    }

    return ByteString.copyFrom(bytes);
  }

  /**
   * Method serializes a primitive to a String. Most types are serialized with the standard toString method.
   * Bytes and byte arrays pass through as they are whilst DATETIME types gets converted to a ISO8601 formatted String.
   * @param type the {@link Row row} field type
   * @param value the value from the {@link Row row}
   * @return a ByteString.
   */
  protected static String primitiveFieldToString(TypeName type, Object value) {

    if (value == null) {
      return "";
    }

    String string;

    switch (type) {
      case BYTE:
        return BaseEncoding.base64().encode(new byte[] {(byte) value});
      case INT16:
      case DECIMAL:
      case INT64:
      case INT32:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BOOLEAN:
        return value.toString();
      case DATETIME:
        ReadableInstant dateTime = ((ReadableInstant) value);
        // Write a ISO8601 formatted String.
        return dateTime.toString();
      case BYTES:
        return BaseEncoding.base64().encode((byte[]) value);
      default:
        throw new UnsupportedOperationException(
            "Unsupported type: " + type + " This method only supports primitives.");
    }
  }

  /**
   * This method generates a String-based Bigtable Rowkey based on the Beam {@link Row} supplied as
   * input. The method filters out all fields containing a metadata key set to "key_order". The
   * value of this metadata field represents the order in which the field was used in Cassandra when
   * constructing the primary key. Example:
   *
   * <p>If you supply the method with a row having three fields named 'key1', 'key2' and 'cell1'
   * with values 'hello', 'world' and '1'. Where fields 'key1', 'key2' have the metadata
   * "key_order",0 and "key_order",1 and defaultRowKeySeparator '#' the method would return:
   * 'hello#world'
   *
   * @param row the row to create a rowkey from.
   * @return the string formatted rowkey.
   */
  private ByteString generateRowKey(Row row) {
    Map<String, String> keyColumns =
        row.getSchema().getFields().stream()
            .filter(
                f ->
                    !f.getType()
                        .getMetadataString(CassandraRowMapperFn.KEY_ORDER_METADATA_KEY)
                        .isEmpty())
            .collect(
                Collectors.toMap(
                    f -> f.getType().getMetadataString(CassandraRowMapperFn.KEY_ORDER_METADATA_KEY),
                    Field::getName,
                    (f, s) -> f,
                    TreeMap::new));

    // Join all values (field names) in order.
    String rowkey =
        keyColumns.entrySet().stream()
            .map(
                e ->
                    primitiveFieldToString(
                        row.getSchema().getField(e.getValue()).getType().getTypeName(),
                        row.getValue(e.getValue())))
            .collect(Collectors.joining(defaultRowKeySeparator.get()));
    byte[] bytes = Bytes.toBytes(rowkey);
    return ByteString.copyFrom(bytes);
  }
}
