/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.bigtable;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.cassandra.Mapper;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;

/**
 * This is an implementation of the Cassandra Mapper interface which allows custom Row Mapping logic
 * to be injected into the Beam CassandraIO. The goal of this implementation is to take a Cassandra
 * Resultset and for each of the rows map them into a BeamRow.
 *
 * @see <a href="https://beam.apache.org/releases/javadoc/2.12
 *     .0/org/apache/beam/sdk/io/cassandra/Mapper.html"> The Cassandra Mapper Interface</a>
 */
class CassandraRowMapperFn implements Mapper<Row>, Serializable {

  private final Session session;
  private final ValueProvider<String> table;
  private final ValueProvider<String> keyspace;
  private Schema schema;
  static final String KEY_ORDER_METADATA_KEY = "key_order";

  /**
   * Constructor used by CassandraRowMapperFactory.
   *
   * @see CassandraRowMapperFactory
   * @see <a href="https://beam.apache.org/releases/javadoc/2.12
   *     .0/org/apache/beam/sdk/io/cassandra/Mapper.html"> The Cassandra Mapper Interface</a>
   * @see org.apache.beam.sdk.values.Row
   * @param session the Cassandra session.
   * @param keyspace the Cassandra keyspace to read data from.
   * @param table the Cassandra table to read from.
   */
  CassandraRowMapperFn(
      Session session, ValueProvider<String> keyspace, ValueProvider<String> table) {
    this.session = session;
    this.table = table;
    this.keyspace = keyspace;
  }

  /**
   * @param resultSet the {@link ResultSet} to map.
   * @return an iterator containing the mapped rows.
   * @see ResultSet
   * @see Row
   */
  @Override
  public Iterator<Row> map(ResultSet resultSet) {
    List<Definition> columnDefinitions = resultSet.getColumnDefinitions().asList();

    // If there is no schema then generate one. We assume all rows have the same schema.
    if (schema == null) {
      // Get the order of the primary keys. We store the order of the primary keys in the field
      // metadata.
      Map<String, Integer> keyOrder =
          CassandraKeyUtils.primaryKeyOrder(session, keyspace.get(), table.get());
      schema = createBeamRowSchema(columnDefinitions, keyOrder);
    }

    List<Row> rows = new ArrayList<>();
    // Loop over each cassandra row and covert this to a Beam row.
    for (com.datastax.driver.core.Row cassandraRow : resultSet) {
      List<Object> values = new ArrayList<>();
      for (int i = 0; i < columnDefinitions.size(); i++) {
        DataType columnType = columnDefinitions.get(i).getType();
        Object columnValue = mapValue(cassandraRow.getObject(i), columnType);
        values.add(columnValue);
      }
      Row row = Row.withSchema(schema).addValues(values).build();

      rows.add(row);
    }
    return rows.iterator();
  }

  /** Not used as this pipeline only reads from cassandra. */
  @Override
  public Future<Void> deleteAsync(Row entity) {
    throw new UnsupportedOperationException();
  }

  /** Not used as this pipeline only reads from cassandra. */
  @Override
  public Future<Void> saveAsync(Row entity) {
    throw new UnsupportedOperationException();
  }

  /**
   * This method creates a Beam {@link Schema schema} based on the list of Definition supplied as
   * input. If a field name matches a key from the keyOrder map (supplied as a input parameter) this
   * denotes that the field was previously part of the Cassandra primary-key. To preserve the order
   * of these primary keys metadata is added to the field on the format "key_order": X - where X is
   * an integer indicating the order of the key in Cassandra.
   *
   * @param definitions a list of field definitions.
   * @param keyOrder the order of the fields that make up the Cassandra row key. key is field name,
   *     value denotes the order from the row-key.
   * @return a {@link Schema schema} schema for all fields supplied as input.
   */
  private Schema createBeamRowSchema(List<Definition> definitions, Map<String, Integer> keyOrder) {

    List<Field> fields =
        definitions.stream()
            .map(def -> Field.of(def.getName(), toBeamRowType(def.getType())).withNullable(true))
            .map(
                fld ->
                    keyOrder.containsKey(fld.getName())
                        ? fld.withType(
                            fld.getType()
                                .withMetadata(
                                    KEY_ORDER_METADATA_KEY, keyOrder.get(fld.getName()).toString()))
                        : fld)
            .collect(Collectors.toList());

    return Schema.builder().addFields(fields).build();
  }

  /**
   * This method takes Objects retrieved from Cassandra as well as their Column type and converts
   * them into Objects that are supported by the Beam {@link Schema} and {@link Row}.
   *
   * <p>The method starts with the complex types {@link List}, {@link Map} and {@link Set}. For all
   * other types it assumes that they are primitives and passes them on.
   *
   * @param object The Cassandra cell value to be converted.
   * @param type The Cassandra Data Type
   * @return the beam compatible object.
   */
  private Object mapValue(Object object, DataType type) {
    DataType.Name typeName = type.getName();

    if (typeName == Name.LIST) {
      DataType innerType = type.getTypeArguments().get(0);
      List list = (List) object;
      // Apply toBeamObject on all items.
      return list.stream()
          .map(value -> toBeamObject(value, innerType))
          .collect(Collectors.toList());
    } else if (typeName == Name.MAP) {
      DataType ktype = type.getTypeArguments().get(0);
      DataType vtype = type.getTypeArguments().get(1);
      Set<Entry> map = ((Map) object).entrySet();
      // Apply toBeamObject on both key and value.
      return map.stream()
          .collect(
              Collectors.toMap(
                  e -> toBeamObject(e.getKey(), ktype), e -> toBeamObject(e.getValue(), vtype)));
    } else if (typeName == Name.SET) {
      DataType innerType = type.getTypeArguments().get(0);
      List list = new ArrayList((Set) object);
      // Apply toBeamObject on all items.
      return list.stream().map(l -> toBeamObject(l, innerType)).collect(Collectors.toList());
    } else {
      return toBeamObject(object, type);
    }
  }

  /**
   * This method converts Cassandra {@link DataType} to Beam {@link FieldType}. Tuples are as of yet
   * not supported as there is no corresponding type in the Beam {@link Schema}.
   *
   * @param type the Cassandra DataType to be converted
   * @return the corresponding Beam Schema field type.
   * @see org.apache.beam.sdk.schemas.Schema.FieldType
   * @see com.datastax.driver.core.DataType
   * @link <a href="https://docs.datastax.com/en/cql/3.3/cql/cql_reference/tupleType.html">Cassandra
   *     Tuple</a>
   */
  private FieldType toBeamRowType(DataType type) {
    DataType.Name n = type.getName();

    switch (n) {
      case TIMESTAMP:
      case DATE:
        return FieldType.DATETIME;
      case BLOB:
        return FieldType.BYTES;
      case BOOLEAN:
        return FieldType.BOOLEAN;
      case DECIMAL:
        return FieldType.DECIMAL;
      case DOUBLE:
        return FieldType.DOUBLE;
      case FLOAT:
        return FieldType.FLOAT;
      case INT:
        return FieldType.INT32;
      case VARINT:
        return FieldType.DECIMAL;
      case SMALLINT:
        return FieldType.INT16;
      case TINYINT:
        return FieldType.BYTE;
      case LIST:
      case SET:
        DataType innerType = type.getTypeArguments().get(0);
        return FieldType.array(toBeamRowType(innerType));
      case MAP:
        DataType kDataType = type.getTypeArguments().get(0);
        DataType vDataType = type.getTypeArguments().get(1);
        FieldType k = toBeamRowType(kDataType);
        FieldType v = toBeamRowType(vDataType);
        return FieldType.map(k, v);
      case VARCHAR:
      case TEXT:
      case INET:
      case UUID:
      case TIMEUUID:
      case ASCII:
        return FieldType.STRING;
      case BIGINT:
      case COUNTER:
      case TIME:
        return FieldType.INT64;
      default:
        throw new UnsupportedOperationException("Datatype " + type.getName() + " not supported.");
    }
  }

  /**
   * Most primitives are represented the same way in Beam and Cassandra however there are a few that
   * differ. This method converts the native representation of timestamps, uuids, varint, dates and
   * IPs to a format which works for the Beam Schema.
   *
   * <p>Dates and Timestamps are returned as DateTime objects whilst UUIDs are converted to Strings.
   * Varint is converted into BigDecimal. The rest simply pass through as they are.
   *
   * @param value The object value as retrieved from Cassandra.
   * @param typeName The Cassandra schema type.
   * @see org.apache.beam.sdk.schemas.Schema.FieldType
   * @return The corresponding representation that works in the Beam schema.
   */
  private Object toBeamObject(Object value, DataType typeName) {
    if (typeName == null || typeName.getName() == null) {
      throw new UnsupportedOperationException(
          "Unspecified Cassandra data type, cannot convert to beam row primitive.");
    }
    switch (typeName.getName()) {
      case TIMESTAMP:
        return new DateTime(value);
      case UUID:
        return value.toString();
      case VARINT:
        return new BigDecimal((BigInteger) value);
      case TIMEUUID:
        return value.toString();
      case DATE:
        LocalDate ld = (LocalDate) value;
        return new DateTime(ld.getYear(), ld.getMonth(), ld.getDay(), 0, 0);
      case INET:
        return ((InetAddress) value).getHostAddress();
      default:
        return value;
    }
  }
}
