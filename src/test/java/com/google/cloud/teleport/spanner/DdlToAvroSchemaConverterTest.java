/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.spanner;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.spanner.common.NumericUtils;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.View;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.Test;

/** Test for {@link DdlToAvroSchemaConverter}. */
public class DdlToAvroSchemaConverterTest {

  @Test
  public void emptyDb() {
    DdlToAvroSchemaConverter converter =
        new DdlToAvroSchemaConverter("spannertest", "booleans", false);
    Ddl empty = Ddl.builder().build();
    assertThat(converter.convert(empty), empty());
  }

  @Test
  public void simple() {
    DdlToAvroSchemaConverter converter =
        new DdlToAvroSchemaConverter("spannertest", "booleans", false);
    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("first_name")
            .string()
            .size(10)
            .endColumn()
            .column("last_name")
            .type(Type.string())
            .max()
            .endColumn()
            .column("full_name")
            .type(Type.string())
            .max()
            .generatedAs("CONCAT(first_name, ' ', last_name)")
            .stored()
            .endColumn()
            .primaryKey()
            .asc("id")
            .desc("last_name")
            .end()
            .indexes(ImmutableList.of("CREATE INDEX `UsersByFirstName` ON `Users` (`first_name`)"))
            .foreignKeys(
                ImmutableList.of(
                    "ALTER TABLE `Users` ADD CONSTRAINT `fk` FOREIGN KEY (`first_name`)"
                        + " REFERENCES `AllowedNames` (`first_name`)"))
            .checkConstraints(ImmutableList.of("CONSTRAINT ck CHECK (`first_name` != `last_name`)"))
            .endTable()
            .build();

    Collection<Schema> result = converter.convert(ddl);
    assertThat(result, hasSize(1));
    Schema avroSchema = result.iterator().next();

    assertThat(avroSchema.getNamespace(), equalTo("spannertest"));
    assertThat(avroSchema.getProp("googleFormatVersion"), equalTo("booleans"));
    assertThat(avroSchema.getProp("googleStorage"), equalTo("CloudSpanner"));

    assertThat(avroSchema.getName(), equalTo("Users"));

    List<Schema.Field> fields = avroSchema.getFields();

    assertThat(fields, hasSize(4));

    assertThat(fields.get(0).name(), equalTo("id"));
    // Not null
    assertThat(fields.get(0).schema().getType(), equalTo(Schema.Type.LONG));
    assertThat(fields.get(0).getProp("sqlType"), equalTo("INT64"));
    assertThat(fields.get(0).getProp("notNull"), equalTo(null));
    assertThat(fields.get(0).getProp("generationExpression"), equalTo(null));
    assertThat(fields.get(0).getProp("stored"), equalTo(null));

    assertThat(fields.get(1).name(), equalTo("first_name"));
    assertThat(fields.get(1).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(1).getProp("sqlType"), equalTo("STRING(10)"));
    assertThat(fields.get(1).getProp("notNull"), equalTo(null));
    assertThat(fields.get(1).getProp("generationExpression"), equalTo(null));
    assertThat(fields.get(1).getProp("stored"), equalTo(null));

    assertThat(fields.get(2).name(), equalTo("last_name"));
    assertThat(fields.get(2).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(2).getProp("sqlType"), equalTo("STRING(MAX)"));
    assertThat(fields.get(2).getProp("notNull"), equalTo(null));
    assertThat(fields.get(2).getProp("generationExpression"), equalTo(null));
    assertThat(fields.get(2).getProp("stored"), equalTo(null));

    assertThat(fields.get(3).name(), equalTo("full_name"));
    assertThat(fields.get(3).schema(), equalTo(Schema.create(Schema.Type.NULL)));
    assertThat(fields.get(3).getProp("sqlType"), equalTo("STRING(MAX)"));
    assertThat(fields.get(3).getProp("notNull"), equalTo("false"));
    assertThat(
        fields.get(3).getProp("generationExpression"),
        equalTo("CONCAT(first_name, ' ', last_name)"));
    assertThat(fields.get(3).getProp("stored"), equalTo("true"));

    // spanner pk
    assertThat(avroSchema.getProp("spannerPrimaryKey_0"), equalTo("`id` ASC"));
    assertThat(avroSchema.getProp("spannerPrimaryKey_1"), equalTo("`last_name` DESC"));
    assertThat(avroSchema.getProp("spannerParent"), nullValue());
    assertThat(avroSchema.getProp("spannerOnDeleteAction"), nullValue());

    assertThat(
        avroSchema.getProp("spannerIndex_0"),
        equalTo("CREATE INDEX `UsersByFirstName` ON `Users` (`first_name`)"));
    assertThat(
        avroSchema.getProp("spannerForeignKey_0"),
        equalTo(
            "ALTER TABLE `Users` ADD CONSTRAINT `fk` FOREIGN KEY (`first_name`)"
                + " REFERENCES `AllowedNames` (`first_name`)"));
    assertThat(
        avroSchema.getProp("spannerCheckConstraint_0"),
        equalTo("CONSTRAINT ck CHECK (`first_name` != `last_name`)"));

    System.out.println(avroSchema.toString(true));
  }

  @Test
  public void invokerRightsView() {
    DdlToAvroSchemaConverter converter =
        new DdlToAvroSchemaConverter("spannertest", "booleans", false);
    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("first_name")
            .string()
            .size(10)
            .endColumn()
            .column("last_name")
            .type(Type.string())
            .max()
            .endColumn()
            .endTable()
            .createView("Names")
            .query("SELECT first_name, last_name FROM Users")
            .security(View.SqlSecurity.INVOKER)
            .endView()
            .build();

    Collection<Schema> result = converter.convert(ddl);
    assertThat(result, hasSize(2));
    Schema avroView = null;
    for (Schema s : result) {
      if (s.getName().equals("Names")) {
        avroView = s;
      }
    }
    assertThat(avroView, notNullValue());

    assertThat(avroView.getNamespace(), equalTo("spannertest"));
    assertThat(avroView.getProp("googleFormatVersion"), equalTo("booleans"));
    assertThat(avroView.getProp("googleStorage"), equalTo("CloudSpanner"));
    assertThat(
        avroView.getProp("spannerViewQuery"), equalTo("SELECT first_name, last_name FROM Users"));
    assertThat(avroView.getProp("spannerViewSecurity"), equalTo("INVOKER"));

    assertThat(avroView.getName(), equalTo("Names"));
  }

  @Test
  public void allTypes() {
    DdlToAvroSchemaConverter converter =
        new DdlToAvroSchemaConverter("spannertest", "booleans", false);
    Ddl ddl =
        Ddl.builder()
            .createTable("AllTYPES")
            .column("bool_field")
            .bool()
            .endColumn()
            .column("int64_field")
            .int64()
            .endColumn()
            .column("float64_field")
            .float64()
            .endColumn()
            .column("string_field")
            .string()
            .max()
            .endColumn()
            .column("bytes_field")
            .bytes()
            .max()
            .endColumn()
            .column("timestamp_field")
            .timestamp()
            .endColumn()
            .column("date_field")
            .date()
            .endColumn()
            .column("numeric_field")
            .numeric()
            .endColumn()
            .column("json_field")
            .json()
            .endColumn()
            .column("arr_bool_field")
            .type(Type.array(Type.bool()))
            .endColumn()
            .column("arr_int64_field")
            .type(Type.array(Type.int64()))
            .endColumn()
            .column("arr_float64_field")
            .type(Type.array(Type.float64()))
            .endColumn()
            .column("arr_string_field")
            .type(Type.array(Type.string()))
            .max()
            .endColumn()
            .column("arr_bytes_field")
            .type(Type.array(Type.bytes()))
            .max()
            .endColumn()
            .column("arr_timestamp_field")
            .type(Type.array(Type.timestamp()))
            .endColumn()
            .column("arr_date_field")
            .type(Type.array(Type.date()))
            .endColumn()
            .column("arr_numeric_field")
            .type(Type.array(Type.numeric()))
            .endColumn()
            .column("arr_json_field")
            .type(Type.array(Type.json()))
            .endColumn()
            .primaryKey()
            .asc("bool_field")
            .end()
            .interleaveInParent("ParentTable")
            .onDeleteCascade()
            .endTable()
            .build();

    Collection<Schema> result = converter.convert(ddl);
    assertThat(result, hasSize(1));
    Schema avroSchema = result.iterator().next();

    assertThat(avroSchema.getNamespace(), equalTo("spannertest"));
    assertThat(avroSchema.getProp("googleFormatVersion"), equalTo("booleans"));
    assertThat(avroSchema.getProp("googleStorage"), equalTo("CloudSpanner"));

    List<Schema.Field> fields = avroSchema.getFields();

    assertThat(fields, hasSize(18));

    assertThat(fields.get(0).name(), equalTo("bool_field"));
    assertThat(fields.get(0).schema(), equalTo(nullableUnion(Schema.Type.BOOLEAN)));
    assertThat(fields.get(0).getProp("sqlType"), equalTo("BOOL"));

    assertThat(fields.get(1).name(), equalTo("int64_field"));
    assertThat(fields.get(1).schema(), equalTo(nullableUnion(Schema.Type.LONG)));
    assertThat(fields.get(1).getProp("sqlType"), equalTo("INT64"));

    assertThat(fields.get(2).name(), equalTo("float64_field"));
    assertThat(fields.get(2).schema(), equalTo(nullableUnion(Schema.Type.DOUBLE)));
    assertThat(fields.get(2).getProp("sqlType"), equalTo("FLOAT64"));

    assertThat(fields.get(3).name(), equalTo("string_field"));
    assertThat(fields.get(3).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(3).getProp("sqlType"), equalTo("STRING(MAX)"));

    assertThat(fields.get(4).name(), equalTo("bytes_field"));
    assertThat(fields.get(4).schema(), equalTo(nullableUnion(Schema.Type.BYTES)));
    assertThat(fields.get(4).getProp("sqlType"), equalTo("BYTES(MAX)"));

    assertThat(fields.get(5).name(), equalTo("timestamp_field"));
    assertThat(fields.get(5).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(5).getProp("sqlType"), equalTo("TIMESTAMP"));

    assertThat(fields.get(6).name(), equalTo("date_field"));
    assertThat(fields.get(6).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(6).getProp("sqlType"), equalTo("DATE"));

    assertThat(fields.get(7).name(), equalTo("numeric_field"));
    assertThat(fields.get(7).schema(), equalTo(nullableNumericUnion()));
    assertThat(fields.get(7).getProp("sqlType"), equalTo("NUMERIC"));

    assertThat(fields.get(8).name(), equalTo("json_field"));
    assertThat(fields.get(8).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(8).getProp("sqlType"), equalTo("JSON"));

    assertThat(fields.get(9).name(), equalTo("arr_bool_field"));
    assertThat(fields.get(9).schema(), equalTo(nullableArray(Schema.Type.BOOLEAN)));
    assertThat(fields.get(9).getProp("sqlType"), equalTo("ARRAY<BOOL>"));

    assertThat(fields.get(10).name(), equalTo("arr_int64_field"));
    assertThat(fields.get(10).schema(), equalTo(nullableArray(Schema.Type.LONG)));
    assertThat(fields.get(10).getProp("sqlType"), equalTo("ARRAY<INT64>"));

    assertThat(fields.get(11).name(), equalTo("arr_float64_field"));
    assertThat(fields.get(11).schema(), equalTo(nullableArray(Schema.Type.DOUBLE)));
    assertThat(fields.get(11).getProp("sqlType"), equalTo("ARRAY<FLOAT64>"));

    assertThat(fields.get(12).name(), equalTo("arr_string_field"));
    assertThat(fields.get(12).schema(), equalTo(nullableArray(Schema.Type.STRING)));
    assertThat(fields.get(12).getProp("sqlType"), equalTo("ARRAY<STRING(MAX)>"));

    assertThat(fields.get(13).name(), equalTo("arr_bytes_field"));
    assertThat(fields.get(13).schema(), equalTo(nullableArray(Schema.Type.BYTES)));
    assertThat(fields.get(13).getProp("sqlType"), equalTo("ARRAY<BYTES(MAX)>"));

    assertThat(fields.get(14).name(), equalTo("arr_timestamp_field"));
    assertThat(fields.get(14).schema(), equalTo(nullableArray(Schema.Type.STRING)));
    assertThat(fields.get(14).getProp("sqlType"), equalTo("ARRAY<TIMESTAMP>"));

    assertThat(fields.get(15).name(), equalTo("arr_date_field"));
    assertThat(fields.get(15).schema(), equalTo(nullableArray(Schema.Type.STRING)));
    assertThat(fields.get(15).getProp("sqlType"), equalTo("ARRAY<DATE>"));

    assertThat(fields.get(16).name(), equalTo("arr_numeric_field"));
    assertThat(fields.get(16).schema(), equalTo(nullableNumericArray()));
    assertThat(fields.get(16).getProp("sqlType"), equalTo("ARRAY<NUMERIC>"));

    assertThat(fields.get(17).name(), equalTo("arr_json_field"));
    assertThat(fields.get(17).schema(), equalTo(nullableArray(Schema.Type.STRING)));
    assertThat(fields.get(17).getProp("sqlType"), equalTo("ARRAY<JSON>"));

    assertThat(avroSchema.getProp("spannerPrimaryKey_0"), equalTo("`bool_field` ASC"));
    assertThat(avroSchema.getProp("spannerParent"), equalTo("ParentTable"));
    assertThat(avroSchema.getProp("spannerOnDeleteAction"), equalTo("cascade"));

    System.out.println(avroSchema.toString(true));
  }

  @Test
  public void timestampLogicalTypeTest() {
    DdlToAvroSchemaConverter converter =
        new DdlToAvroSchemaConverter("spannertest", "booleans", true);
    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("timestamp_field")
            .timestamp()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    Collection<Schema> result = converter.convert(ddl);
    assertThat(result, hasSize(1));
    Schema avroSchema = result.iterator().next();

    assertThat(avroSchema.getNamespace(), equalTo("spannertest"));
    assertThat(avroSchema.getProp("googleFormatVersion"), equalTo("booleans"));
    assertThat(avroSchema.getProp("googleStorage"), equalTo("CloudSpanner"));

    assertThat(avroSchema.getName(), equalTo("Users"));

    List<Schema.Field> fields = avroSchema.getFields();

    assertThat(fields, hasSize(2));

    assertThat(fields.get(0).name(), equalTo("id"));
    // Not null
    assertThat(fields.get(0).schema().getType(), equalTo(Schema.Type.LONG));
    assertThat(fields.get(0).getProp("sqlType"), equalTo("INT64"));
    assertThat(fields.get(0).getProp("notNull"), equalTo(null));
    assertThat(fields.get(0).getProp("generationExpression"), equalTo(null));
    assertThat(fields.get(0).getProp("stored"), equalTo(null));

    assertThat(fields.get(1).name(), equalTo("timestamp_field"));
    assertThat(fields.get(1).schema(), equalTo(nullableTimestampUnion()));
    assertThat(fields.get(1).getProp("sqlType"), equalTo("TIMESTAMP"));
  }

  private Schema nullableUnion(Schema.Type s) {
    return Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(s));
  }

  private Schema nullableNumericUnion() {
    return Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        LogicalTypes.decimal(NumericUtils.PRECISION, NumericUtils.SCALE)
            .addToSchema(Schema.create(Schema.Type.BYTES)));
  }

  private Schema nullableTimestampUnion() {
    return Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)));
  }

  private Schema nullableArray(Schema.Type s) {
    return Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        Schema.createArray(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(s))));
  }

  private Schema nullableNumericArray() {
    return Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        Schema.createArray(
            Schema.createUnion(
                Schema.create(Schema.Type.NULL),
                LogicalTypes.decimal(NumericUtils.PRECISION, NumericUtils.SCALE)
                    .addToSchema(Schema.create(Schema.Type.BYTES)))));
  }
}
