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

import static com.google.cloud.teleport.spanner.AvroUtil.DEFAULT_EXPRESSION;
import static com.google.cloud.teleport.spanner.AvroUtil.GENERATION_EXPRESSION;
import static com.google.cloud.teleport.spanner.AvroUtil.GOOGLE_FORMAT_VERSION;
import static com.google.cloud.teleport.spanner.AvroUtil.GOOGLE_STORAGE;
import static com.google.cloud.teleport.spanner.AvroUtil.INPUT;
import static com.google.cloud.teleport.spanner.AvroUtil.NOT_NULL;
import static com.google.cloud.teleport.spanner.AvroUtil.OUTPUT;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_CHANGE_STREAM_FOR_CLAUSE;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_CHECK_CONSTRAINT;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_ENTITY;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_ENTITY_MODEL;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_FOREIGN_KEY;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_INDEX;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_ON_DELETE_ACTION;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_OPTION;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_PARENT;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_PRIMARY_KEY;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_REMOTE;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_SEQUENCE_COUNTER_START;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_SEQUENCE_KIND;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_SEQUENCE_SKIP_RANGE_MAX;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_SEQUENCE_SKIP_RANGE_MIN;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_VIEW_QUERY;
import static com.google.cloud.teleport.spanner.AvroUtil.SPANNER_VIEW_SECURITY;
import static com.google.cloud.teleport.spanner.AvroUtil.SQL_TYPE;
import static com.google.cloud.teleport.spanner.AvroUtil.STORED;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.common.NumericUtils;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.View;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Iterator;
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
  public void pgEmptyDb() {
    DdlToAvroSchemaConverter converter =
        new DdlToAvroSchemaConverter("spannertest", "booleans", false);
    Ddl empty = Ddl.builder(Dialect.POSTGRESQL).build();
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
            .defaultExpression("'John'")
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
            .column("gen_id")
            .int64()
            .notNull()
            .generatedAs("MOD(id+1, 64)")
            .stored()
            .endColumn()
            .primaryKey()
            .asc("id")
            .asc("gen_id")
            .desc("last_name")
            .end()
            .indexes(ImmutableList.of("CREATE INDEX `UsersByFirstName` ON `Users` (`first_name`)"))
            .foreignKeys(
                ImmutableList.of(
                    "ALTER TABLE `Users` ADD CONSTRAINT `fk` FOREIGN KEY (`first_name`)"
                        + " REFERENCES `AllowedNames` (`first_name`)",
                    "ALTER TABLE `Users` ADD CONSTRAINT `fk_odc` FOREIGN KEY (`last_name`)"
                        + " REFERENCES `AllowedNames` (`last_name`) ON DELETE CASCADE"))
            .checkConstraints(ImmutableList.of("CONSTRAINT ck CHECK (`first_name` != `last_name`)"))
            .endTable()
            .build();

    Collection<Schema> result = converter.convert(ddl);
    assertThat(result, hasSize(1));
    Schema avroSchema = result.iterator().next();

    assertThat(avroSchema.getNamespace(), equalTo("spannertest"));
    assertThat(avroSchema.getProp(GOOGLE_FORMAT_VERSION), equalTo("booleans"));
    assertThat(avroSchema.getProp(GOOGLE_STORAGE), equalTo("CloudSpanner"));

    assertThat(avroSchema.getName(), equalTo("Users"));

    List<Schema.Field> fields = avroSchema.getFields();

    assertThat(fields, hasSize(5));

    assertThat(fields.get(0).name(), equalTo("id"));
    // Not null
    assertThat(fields.get(0).schema().getType(), equalTo(Schema.Type.LONG));
    assertThat(fields.get(0).getProp(SQL_TYPE), equalTo("INT64"));
    assertThat(fields.get(0).getProp(NOT_NULL), equalTo(null));
    assertThat(fields.get(0).getProp(GENERATION_EXPRESSION), equalTo(null));
    assertThat(fields.get(0).getProp(STORED), equalTo(null));
    assertThat(fields.get(0).getProp(DEFAULT_EXPRESSION), equalTo(null));

    assertThat(fields.get(1).name(), equalTo("first_name"));
    assertThat(fields.get(1).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(1).getProp(SQL_TYPE), equalTo("STRING(10)"));
    assertThat(fields.get(1).getProp(NOT_NULL), equalTo(null));
    assertThat(fields.get(1).getProp(GENERATION_EXPRESSION), equalTo(null));
    assertThat(fields.get(1).getProp(STORED), equalTo(null));
    assertThat(fields.get(1).getProp(DEFAULT_EXPRESSION), equalTo("'John'"));

    assertThat(fields.get(2).name(), equalTo("last_name"));
    assertThat(fields.get(2).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(2).getProp(SQL_TYPE), equalTo("STRING(MAX)"));
    assertThat(fields.get(2).getProp(NOT_NULL), equalTo(null));
    assertThat(fields.get(2).getProp(GENERATION_EXPRESSION), equalTo(null));
    assertThat(fields.get(2).getProp(STORED), equalTo(null));
    assertThat(fields.get(2).getProp(DEFAULT_EXPRESSION), equalTo(null));

    assertThat(fields.get(3).name(), equalTo("full_name"));
    assertThat(fields.get(3).schema(), equalTo(Schema.create(Schema.Type.NULL)));
    assertThat(fields.get(3).getProp(SQL_TYPE), equalTo("STRING(MAX)"));
    assertThat(fields.get(3).getProp(NOT_NULL), equalTo("false"));
    assertThat(
        fields.get(3).getProp(GENERATION_EXPRESSION),
        equalTo("CONCAT(first_name, ' ', last_name)"));
    assertThat(fields.get(3).getProp(STORED), equalTo("true"));
    assertThat(fields.get(3).getProp(DEFAULT_EXPRESSION), equalTo(null));

    assertThat(fields.get(4).name(), equalTo("gen_id"));
    assertThat(fields.get(4).schema(), equalTo(Schema.create(Schema.Type.NULL)));
    assertThat(fields.get(4).getProp(SQL_TYPE), equalTo("INT64"));
    assertThat(fields.get(4).getProp(NOT_NULL), equalTo("true"));
    assertThat(fields.get(4).getProp(GENERATION_EXPRESSION), equalTo("MOD(id+1, 64)"));
    assertThat(fields.get(4).getProp(STORED), equalTo("true"));
    assertThat(fields.get(4).getProp(DEFAULT_EXPRESSION), equalTo(null));

    // spanner pk
    assertThat(avroSchema.getProp(SPANNER_PRIMARY_KEY + "_0"), equalTo("`id` ASC"));
    assertThat(avroSchema.getProp(SPANNER_PRIMARY_KEY + "_1"), equalTo("`gen_id` ASC"));
    assertThat(avroSchema.getProp(SPANNER_PRIMARY_KEY + "_2"), equalTo("`last_name` DESC"));
    assertThat(avroSchema.getProp(SPANNER_PARENT), nullValue());
    assertThat(avroSchema.getProp(SPANNER_ON_DELETE_ACTION), nullValue());

    assertThat(
        avroSchema.getProp(SPANNER_INDEX + "0"),
        equalTo("CREATE INDEX `UsersByFirstName` ON `Users` (`first_name`)"));
    assertThat(
        avroSchema.getProp(SPANNER_FOREIGN_KEY + "0"),
        equalTo(
            "ALTER TABLE `Users` ADD CONSTRAINT `fk` FOREIGN KEY (`first_name`)"
                + " REFERENCES `AllowedNames` (`first_name`)"));
    assertThat(
        avroSchema.getProp(SPANNER_FOREIGN_KEY + "1"),
        equalTo(
            "ALTER TABLE `Users` ADD CONSTRAINT `fk_odc` FOREIGN KEY (`last_name`)"
                + " REFERENCES `AllowedNames` (`last_name`) ON DELETE CASCADE"));
    assertThat(
        avroSchema.getProp(SPANNER_CHECK_CONSTRAINT + "0"),
        equalTo("CONSTRAINT ck CHECK (`first_name` != `last_name`)"));

    System.out.println(avroSchema.toString(true));
  }

  @Test
  public void pgSimple() {
    DdlToAvroSchemaConverter converter =
        new DdlToAvroSchemaConverter("spannertest", "booleans", false);
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("Users")
            .column("id")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("first_name")
            .pgVarchar()
            .size(10)
            .defaultExpression("'John'")
            .endColumn()
            .column("last_name")
            .type(Type.pgVarchar())
            .max()
            .endColumn()
            .column("full_name")
            .type(Type.pgVarchar())
            .max()
            .generatedAs("CONCAT(first_name, ' ', last_name)")
            .stored()
            .endColumn()
            .column("gen_id")
            .pgInt8()
            .notNull()
            .generatedAs("MOD(id+1, 64)")
            .stored()
            .endColumn()
            .primaryKey()
            .asc("id")
            .asc("gen_id")
            .asc("last_name")
            .end()
            .indexes(
                ImmutableList.of("CREATE INDEX \"UsersByFirstName\" ON \"Users\" (\"first_name\")"))
            .foreignKeys(
                ImmutableList.of(
                    "ALTER TABLE \"Users\" ADD CONSTRAINT \"fk\" FOREIGN KEY (\"first_name\")"
                        + " REFERENCES \"AllowedNames\" (\"first_name\")",
                    "ALTER TABLE \"Users\" ADD CONSTRAINT \"fk_odc\" FOREIGN KEY (\"last_name\")"
                        + " REFERENCES \"AllowedNames\" (\"last_name\") ON DELETE CASCADE"))
            .checkConstraints(
                ImmutableList.of("CONSTRAINT ck CHECK (\"first_name\" != \"last_name\")"))
            .endTable()
            .build();

    Collection<Schema> result = converter.convert(ddl);
    assertThat(result, hasSize(1));
    Schema avroSchema = result.iterator().next();

    assertThat(avroSchema.getNamespace(), equalTo("spannertest"));
    assertThat(avroSchema.getProp(GOOGLE_FORMAT_VERSION), equalTo("booleans"));
    assertThat(avroSchema.getProp(GOOGLE_STORAGE), equalTo("CloudSpanner"));

    assertThat(avroSchema.getName(), equalTo("Users"));

    List<Schema.Field> fields = avroSchema.getFields();

    assertThat(fields, hasSize(5));

    assertThat(fields.get(0).name(), equalTo("id"));
    // Not null
    assertThat(fields.get(0).schema().getType(), equalTo(Schema.Type.LONG));
    assertThat(fields.get(0).getProp(SQL_TYPE), equalTo("bigint"));
    assertThat(fields.get(0).getProp(NOT_NULL), equalTo(null));
    assertThat(fields.get(0).getProp(GENERATION_EXPRESSION), equalTo(null));
    assertThat(fields.get(0).getProp(STORED), equalTo(null));
    assertThat(fields.get(0).getProp(DEFAULT_EXPRESSION), equalTo(null));

    assertThat(fields.get(1).name(), equalTo("first_name"));
    assertThat(fields.get(1).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(1).getProp(SQL_TYPE), equalTo("character varying(10)"));
    assertThat(fields.get(1).getProp(NOT_NULL), equalTo(null));
    assertThat(fields.get(1).getProp(GENERATION_EXPRESSION), equalTo(null));
    assertThat(fields.get(1).getProp(STORED), equalTo(null));
    assertThat(fields.get(1).getProp(DEFAULT_EXPRESSION), equalTo("'John'"));

    assertThat(fields.get(2).name(), equalTo("last_name"));
    assertThat(fields.get(2).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(2).getProp(SQL_TYPE), equalTo("character varying"));
    assertThat(fields.get(2).getProp(NOT_NULL), equalTo(null));
    assertThat(fields.get(2).getProp(GENERATION_EXPRESSION), equalTo(null));
    assertThat(fields.get(2).getProp(STORED), equalTo(null));
    assertThat(fields.get(2).getProp(DEFAULT_EXPRESSION), equalTo(null));

    assertThat(fields.get(3).name(), equalTo("full_name"));
    assertThat(fields.get(3).schema(), equalTo(Schema.create(Schema.Type.NULL)));
    assertThat(fields.get(3).getProp(SQL_TYPE), equalTo("character varying"));
    assertThat(fields.get(3).getProp(NOT_NULL), equalTo("false"));
    assertThat(
        fields.get(3).getProp(GENERATION_EXPRESSION),
        equalTo("CONCAT(first_name, ' ', last_name)"));
    assertThat(fields.get(3).getProp(STORED), equalTo("true"));
    assertThat(fields.get(3).getProp(DEFAULT_EXPRESSION), equalTo(null));

    assertThat(fields.get(4).name(), equalTo("gen_id"));
    assertThat(fields.get(4).schema(), equalTo(Schema.create(Schema.Type.NULL)));
    assertThat(fields.get(4).getProp(SQL_TYPE), equalTo("bigint"));
    assertThat(fields.get(4).getProp(NOT_NULL), equalTo("true"));
    assertThat(fields.get(4).getProp(GENERATION_EXPRESSION), equalTo("MOD(id+1, 64)"));
    assertThat(fields.get(4).getProp(STORED), equalTo("true"));
    assertThat(fields.get(4).getProp(DEFAULT_EXPRESSION), equalTo(null));

    // spanner pk
    assertThat(avroSchema.getProp(SPANNER_PRIMARY_KEY + "_0"), equalTo("\"id\" ASC"));
    assertThat(avroSchema.getProp(SPANNER_PRIMARY_KEY + "_1"), equalTo("\"gen_id\" ASC"));
    assertThat(avroSchema.getProp(SPANNER_PRIMARY_KEY + "_2"), equalTo("\"last_name\" ASC"));
    assertThat(avroSchema.getProp(SPANNER_PARENT), nullValue());
    assertThat(avroSchema.getProp(SPANNER_ON_DELETE_ACTION), nullValue());

    assertThat(
        avroSchema.getProp(SPANNER_INDEX + "0"),
        equalTo("CREATE INDEX \"UsersByFirstName\" ON \"Users\" (\"first_name\")"));
    assertThat(
        avroSchema.getProp(SPANNER_FOREIGN_KEY + "0"),
        equalTo(
            "ALTER TABLE \"Users\" ADD CONSTRAINT \"fk\" FOREIGN KEY (\"first_name\")"
                + " REFERENCES \"AllowedNames\" (\"first_name\")"));
    assertThat(
        avroSchema.getProp(SPANNER_FOREIGN_KEY + "1"),
        equalTo(
            "ALTER TABLE \"Users\" ADD CONSTRAINT \"fk_odc\" FOREIGN KEY (\"last_name\")"
                + " REFERENCES \"AllowedNames\" (\"last_name\") ON DELETE CASCADE"));
    assertThat(
        avroSchema.getProp(SPANNER_CHECK_CONSTRAINT + "0"),
        equalTo("CONSTRAINT ck CHECK (\"first_name\" != \"last_name\")"));
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
    assertThat(avroView.getProp(GOOGLE_FORMAT_VERSION), equalTo("booleans"));
    assertThat(avroView.getProp(GOOGLE_STORAGE), equalTo("CloudSpanner"));
    assertThat(
        avroView.getProp(SPANNER_VIEW_QUERY), equalTo("SELECT first_name, last_name FROM Users"));
    assertThat(avroView.getProp(SPANNER_VIEW_SECURITY), equalTo("INVOKER"));

    assertThat(avroView.getName(), equalTo("Names"));
  }

  @Test
  public void pgInvokerRightsView() {
    DdlToAvroSchemaConverter converter =
        new DdlToAvroSchemaConverter("spannertest", "booleans", false);
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("Users")
            .column("id")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("first_name")
            .pgVarchar()
            .size(10)
            .endColumn()
            .column("last_name")
            .type(Type.pgVarchar())
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
    assertThat(avroView.getProp(GOOGLE_FORMAT_VERSION), equalTo("booleans"));
    assertThat(avroView.getProp(GOOGLE_STORAGE), equalTo("CloudSpanner"));
    assertThat(
        avroView.getProp(SPANNER_VIEW_QUERY), equalTo("SELECT first_name, last_name FROM Users"));
    assertThat(avroView.getProp(SPANNER_VIEW_SECURITY), equalTo("INVOKER"));

    assertThat(avroView.getName(), equalTo("Names"));
  }

  @Test
  public void definerRightsView() {
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
            .security(View.SqlSecurity.DEFINER)
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
    assertThat(avroView.getProp("spannerViewSecurity"), equalTo("DEFINER"));

    assertThat(avroView.getName(), equalTo("Names"));
  }

  @Test
  public void pgDefinerRightsView() {
    DdlToAvroSchemaConverter converter =
        new DdlToAvroSchemaConverter("spannertest", "booleans", false);
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("Users")
            .column("id")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("first_name")
            .pgVarchar()
            .size(10)
            .endColumn()
            .column("last_name")
            .type(Type.pgVarchar())
            .max()
            .endColumn()
            .endTable()
            .createView("Names")
            .query("SELECT first_name, last_name FROM Users")
            .security(View.SqlSecurity.DEFINER)
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
    assertThat(avroView.getProp("spannerViewSecurity"), equalTo("DEFINER"));

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
    assertThat(avroSchema.getProp(GOOGLE_FORMAT_VERSION), equalTo("booleans"));
    assertThat(avroSchema.getProp(GOOGLE_STORAGE), equalTo("CloudSpanner"));

    List<Schema.Field> fields = avroSchema.getFields();

    assertThat(fields, hasSize(18));

    assertThat(fields.get(0).name(), equalTo("bool_field"));
    assertThat(fields.get(0).schema(), equalTo(nullableUnion(Schema.Type.BOOLEAN)));
    assertThat(fields.get(0).getProp(SQL_TYPE), equalTo("BOOL"));

    assertThat(fields.get(1).name(), equalTo("int64_field"));
    assertThat(fields.get(1).schema(), equalTo(nullableUnion(Schema.Type.LONG)));
    assertThat(fields.get(1).getProp(SQL_TYPE), equalTo("INT64"));

    assertThat(fields.get(2).name(), equalTo("float64_field"));
    assertThat(fields.get(2).schema(), equalTo(nullableUnion(Schema.Type.DOUBLE)));
    assertThat(fields.get(2).getProp(SQL_TYPE), equalTo("FLOAT64"));

    assertThat(fields.get(3).name(), equalTo("string_field"));
    assertThat(fields.get(3).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(3).getProp(SQL_TYPE), equalTo("STRING(MAX)"));

    assertThat(fields.get(4).name(), equalTo("bytes_field"));
    assertThat(fields.get(4).schema(), equalTo(nullableUnion(Schema.Type.BYTES)));
    assertThat(fields.get(4).getProp(SQL_TYPE), equalTo("BYTES(MAX)"));

    assertThat(fields.get(5).name(), equalTo("timestamp_field"));
    assertThat(fields.get(5).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(5).getProp(SQL_TYPE), equalTo("TIMESTAMP"));

    assertThat(fields.get(6).name(), equalTo("date_field"));
    assertThat(fields.get(6).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(6).getProp(SQL_TYPE), equalTo("DATE"));

    assertThat(fields.get(7).name(), equalTo("numeric_field"));
    assertThat(fields.get(7).schema(), equalTo(nullableNumericUnion()));
    assertThat(fields.get(7).getProp(SQL_TYPE), equalTo("NUMERIC"));

    assertThat(fields.get(8).name(), equalTo("json_field"));
    assertThat(fields.get(8).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(8).getProp(SQL_TYPE), equalTo("JSON"));

    assertThat(fields.get(9).name(), equalTo("arr_bool_field"));
    assertThat(fields.get(9).schema(), equalTo(nullableArray(Schema.Type.BOOLEAN)));
    assertThat(fields.get(9).getProp(SQL_TYPE), equalTo("ARRAY<BOOL>"));

    assertThat(fields.get(10).name(), equalTo("arr_int64_field"));
    assertThat(fields.get(10).schema(), equalTo(nullableArray(Schema.Type.LONG)));
    assertThat(fields.get(10).getProp(SQL_TYPE), equalTo("ARRAY<INT64>"));

    assertThat(fields.get(11).name(), equalTo("arr_float64_field"));
    assertThat(fields.get(11).schema(), equalTo(nullableArray(Schema.Type.DOUBLE)));
    assertThat(fields.get(11).getProp(SQL_TYPE), equalTo("ARRAY<FLOAT64>"));

    assertThat(fields.get(12).name(), equalTo("arr_string_field"));
    assertThat(fields.get(12).schema(), equalTo(nullableArray(Schema.Type.STRING)));
    assertThat(fields.get(12).getProp(SQL_TYPE), equalTo("ARRAY<STRING(MAX)>"));

    assertThat(fields.get(13).name(), equalTo("arr_bytes_field"));
    assertThat(fields.get(13).schema(), equalTo(nullableArray(Schema.Type.BYTES)));
    assertThat(fields.get(13).getProp(SQL_TYPE), equalTo("ARRAY<BYTES(MAX)>"));

    assertThat(fields.get(14).name(), equalTo("arr_timestamp_field"));
    assertThat(fields.get(14).schema(), equalTo(nullableArray(Schema.Type.STRING)));
    assertThat(fields.get(14).getProp(SQL_TYPE), equalTo("ARRAY<TIMESTAMP>"));

    assertThat(fields.get(15).name(), equalTo("arr_date_field"));
    assertThat(fields.get(15).schema(), equalTo(nullableArray(Schema.Type.STRING)));
    assertThat(fields.get(15).getProp(SQL_TYPE), equalTo("ARRAY<DATE>"));

    assertThat(fields.get(16).name(), equalTo("arr_numeric_field"));
    assertThat(fields.get(16).schema(), equalTo(nullableNumericArray()));
    assertThat(fields.get(16).getProp(SQL_TYPE), equalTo("ARRAY<NUMERIC>"));

    assertThat(fields.get(17).name(), equalTo("arr_json_field"));
    assertThat(fields.get(17).schema(), equalTo(nullableArray(Schema.Type.STRING)));
    assertThat(fields.get(17).getProp(SQL_TYPE), equalTo("ARRAY<JSON>"));

    assertThat(avroSchema.getProp(SPANNER_PRIMARY_KEY + "_0"), equalTo("`bool_field` ASC"));
    assertThat(avroSchema.getProp(SPANNER_PARENT), equalTo("ParentTable"));
    assertThat(avroSchema.getProp(SPANNER_ON_DELETE_ACTION), equalTo("cascade"));

    System.out.println(avroSchema.toString(true));
  }

  @Test
  public void pgAllTypes() {
    DdlToAvroSchemaConverter converter =
        new DdlToAvroSchemaConverter("spannertest", "booleans", false);
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("AllTYPES")
            .column("bool_field")
            .pgBool()
            .endColumn()
            .column("int8_field")
            .pgInt8()
            .endColumn()
            .column("float8_field")
            .pgFloat8()
            .endColumn()
            .column("varchar_field")
            .pgVarchar()
            .max()
            .endColumn()
            .column("bytea_field")
            .pgBytea()
            .endColumn()
            .column("timestamptz_field")
            .pgTimestamptz()
            .endColumn()
            .column("numeric_field")
            .pgNumeric()
            .endColumn()
            .column("text_field")
            .pgText()
            .endColumn()
            .column("date_field")
            .pgDate()
            .endColumn()
            .column("commit_timestamp_field")
            .pgSpannerCommitTimestamp()
            .endColumn()
            .column("arr_bool_field")
            .type(Type.pgArray(Type.pgBool()))
            .endColumn()
            .column("arr_int8_field")
            .type(Type.pgArray(Type.pgInt8()))
            .endColumn()
            .column("arr_float8_field")
            .type(Type.pgArray(Type.pgFloat8()))
            .endColumn()
            .column("arr_varchar_field")
            .type(Type.pgArray(Type.pgVarchar()))
            .max()
            .endColumn()
            .column("arr_bytea_field")
            .type(Type.pgArray(Type.pgBytea()))
            .endColumn()
            .column("arr_timestamptz_field")
            .type(Type.pgArray(Type.pgTimestamptz()))
            .endColumn()
            .column("arr_numeric_field")
            .type(Type.pgArray(Type.pgNumeric()))
            .endColumn()
            .column("arr_text_field")
            .type(Type.pgArray(Type.pgText()))
            .endColumn()
            .column("arr_date_field")
            .type(Type.pgArray(Type.pgDate()))
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
    assertThat(avroSchema.getProp(GOOGLE_FORMAT_VERSION), equalTo("booleans"));
    assertThat(avroSchema.getProp(GOOGLE_STORAGE), equalTo("CloudSpanner"));

    List<Schema.Field> fields = avroSchema.getFields();

    assertThat(fields, hasSize(19));

    assertThat(fields.get(0).name(), equalTo("bool_field"));
    assertThat(fields.get(0).schema(), equalTo(nullableUnion(Schema.Type.BOOLEAN)));
    assertThat(fields.get(0).getProp(SQL_TYPE), equalTo("boolean"));

    assertThat(fields.get(1).name(), equalTo("int8_field"));
    assertThat(fields.get(1).schema(), equalTo(nullableUnion(Schema.Type.LONG)));
    assertThat(fields.get(1).getProp(SQL_TYPE), equalTo("bigint"));

    assertThat(fields.get(2).name(), equalTo("float8_field"));
    assertThat(fields.get(2).schema(), equalTo(nullableUnion(Schema.Type.DOUBLE)));
    assertThat(fields.get(2).getProp(SQL_TYPE), equalTo("double precision"));

    assertThat(fields.get(3).name(), equalTo("varchar_field"));
    assertThat(fields.get(3).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(3).getProp(SQL_TYPE), equalTo("character varying"));

    assertThat(fields.get(4).name(), equalTo("bytea_field"));
    assertThat(fields.get(4).schema(), equalTo(nullableUnion(Schema.Type.BYTES)));
    assertThat(fields.get(4).getProp(SQL_TYPE), equalTo("bytea"));

    assertThat(fields.get(5).name(), equalTo("timestamptz_field"));
    assertThat(fields.get(5).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(5).getProp(SQL_TYPE), equalTo("timestamp with time zone"));

    assertThat(fields.get(6).name(), equalTo("numeric_field"));
    assertThat(fields.get(6).schema(), equalTo(nullablePgNumericUnion()));
    assertThat(fields.get(6).getProp(SQL_TYPE), equalTo("numeric"));

    assertThat(fields.get(7).name(), equalTo("text_field"));
    assertThat(fields.get(7).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(7).getProp(SQL_TYPE), equalTo("text"));

    assertThat(fields.get(8).name(), equalTo("date_field"));
    assertThat(fields.get(8).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(8).getProp(SQL_TYPE), equalTo("date"));

    assertThat(fields.get(9).name(), equalTo("commit_timestamp_field"));
    assertThat(fields.get(9).schema(), equalTo(nullableUnion(Schema.Type.STRING)));
    assertThat(fields.get(9).getProp(SQL_TYPE), equalTo("spanner.commit_timestamp"));

    assertThat(fields.get(10).name(), equalTo("arr_bool_field"));
    assertThat(fields.get(10).schema(), equalTo(nullableArray(Schema.Type.BOOLEAN)));
    assertThat(fields.get(10).getProp(SQL_TYPE), equalTo("boolean[]"));

    assertThat(fields.get(11).name(), equalTo("arr_int8_field"));
    assertThat(fields.get(11).schema(), equalTo(nullableArray(Schema.Type.LONG)));
    assertThat(fields.get(11).getProp(SQL_TYPE), equalTo("bigint[]"));

    assertThat(fields.get(12).name(), equalTo("arr_float8_field"));
    assertThat(fields.get(12).schema(), equalTo(nullableArray(Schema.Type.DOUBLE)));
    assertThat(fields.get(12).getProp(SQL_TYPE), equalTo("double precision[]"));

    assertThat(fields.get(13).name(), equalTo("arr_varchar_field"));
    assertThat(fields.get(13).schema(), equalTo(nullableArray(Schema.Type.STRING)));
    assertThat(fields.get(13).getProp(SQL_TYPE), equalTo("character varying[]"));

    assertThat(fields.get(14).name(), equalTo("arr_bytea_field"));
    assertThat(fields.get(14).schema(), equalTo(nullableArray(Schema.Type.BYTES)));
    assertThat(fields.get(14).getProp(SQL_TYPE), equalTo("bytea[]"));

    assertThat(fields.get(15).name(), equalTo("arr_timestamptz_field"));
    assertThat(fields.get(15).schema(), equalTo(nullableArray(Schema.Type.STRING)));
    assertThat(fields.get(15).getProp(SQL_TYPE), equalTo("timestamp with time zone[]"));

    assertThat(fields.get(16).name(), equalTo("arr_numeric_field"));
    assertThat(fields.get(16).schema(), equalTo(nullablePgNumericArray()));
    assertThat(fields.get(16).getProp(SQL_TYPE), equalTo("numeric[]"));

    assertThat(fields.get(17).name(), equalTo("arr_text_field"));
    assertThat(fields.get(17).schema(), equalTo(nullableArray(Schema.Type.STRING)));
    assertThat(fields.get(17).getProp(SQL_TYPE), equalTo("text[]"));

    assertThat(fields.get(18).name(), equalTo("arr_date_field"));
    assertThat(fields.get(18).schema(), equalTo(nullableArray(Schema.Type.STRING)));
    assertThat(fields.get(18).getProp(SQL_TYPE), equalTo("date[]"));

    assertThat(avroSchema.getProp(SPANNER_PRIMARY_KEY + "_0"), equalTo("\"bool_field\" ASC"));
    assertThat(avroSchema.getProp(SPANNER_PARENT), equalTo("ParentTable"));
    assertThat(avroSchema.getProp(SPANNER_ON_DELETE_ACTION), equalTo("cascade"));
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
    assertThat(avroSchema.getProp(GOOGLE_FORMAT_VERSION), equalTo("booleans"));
    assertThat(avroSchema.getProp(GOOGLE_STORAGE), equalTo("CloudSpanner"));

    assertThat(avroSchema.getName(), equalTo("Users"));

    List<Schema.Field> fields = avroSchema.getFields();

    assertThat(fields, hasSize(2));

    assertThat(fields.get(0).name(), equalTo("id"));
    // Not null
    assertThat(fields.get(0).schema().getType(), equalTo(Schema.Type.LONG));
    assertThat(fields.get(0).getProp(SQL_TYPE), equalTo("INT64"));
    assertThat(fields.get(0).getProp(NOT_NULL), equalTo(null));
    assertThat(fields.get(0).getProp(GENERATION_EXPRESSION), equalTo(null));
    assertThat(fields.get(0).getProp(STORED), equalTo(null));

    assertThat(fields.get(1).name(), equalTo("timestamp_field"));
    assertThat(fields.get(1).schema(), equalTo(nullableTimestampUnion()));
    assertThat(fields.get(1).getProp(SQL_TYPE), equalTo("TIMESTAMP"));
  }

  @Test
  public void pgTimestampLogicalTypeTest() {
    DdlToAvroSchemaConverter converter =
        new DdlToAvroSchemaConverter("spannertest", "booleans", true);
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("Users")
            .column("id")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("timestamp_field")
            .pgTimestamptz()
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
    assertThat(avroSchema.getProp(GOOGLE_FORMAT_VERSION), equalTo("booleans"));
    assertThat(avroSchema.getProp(GOOGLE_STORAGE), equalTo("CloudSpanner"));

    assertThat(avroSchema.getName(), equalTo("Users"));

    List<Schema.Field> fields = avroSchema.getFields();

    assertThat(fields, hasSize(2));

    assertThat(fields.get(0).name(), equalTo("id"));
    // Not null
    assertThat(fields.get(0).schema().getType(), equalTo(Schema.Type.LONG));
    assertThat(fields.get(0).getProp(SQL_TYPE), equalTo("bigint"));
    assertThat(fields.get(0).getProp(NOT_NULL), equalTo(null));
    assertThat(fields.get(0).getProp(GENERATION_EXPRESSION), equalTo(null));
    assertThat(fields.get(0).getProp(STORED), equalTo(null));

    assertThat(fields.get(1).name(), equalTo("timestamp_field"));
    assertThat(fields.get(1).schema(), equalTo(nullableTimestampUnion()));
    assertThat(fields.get(1).getProp(SQL_TYPE), equalTo("timestamp with time zone"));
  }

  @Test
  public void models() {
    DdlToAvroSchemaConverter converter =
        new DdlToAvroSchemaConverter("spannertest", "booleans", true);
    Ddl ddl =
        Ddl.builder()
            .createModel("ModelAll")
            .inputColumn("i1")
            .type(Type.bool())
            .size(-1)
            .columnOptions(ImmutableList.of("required=FALSE"))
            .endInputColumn()
            .inputColumn("i2")
            .type(Type.string())
            .size(-1)
            .endInputColumn()
            .outputColumn("o1")
            .type(Type.int64())
            .size(-1)
            .columnOptions(ImmutableList.of("required=TRUE"))
            .endOutputColumn()
            .outputColumn("o2")
            .type(Type.float64())
            .size(-1)
            .endOutputColumn()
            .remote(true)
            .options(ImmutableList.of("endpoint=\"test\""))
            .endModel()
            .createModel("ModelMin")
            .remote(false)
            .inputColumn("i1")
            .type(Type.bool())
            .size(-1)
            .endInputColumn()
            .outputColumn("o1")
            .type(Type.int64())
            .size(-1)
            .endOutputColumn()
            .endModel()
            .build();

    Collection<Schema> result = converter.convert(ddl);
    assertThat(result, hasSize(2));

    Iterator<Schema> iterator = result.iterator();
    Schema s = iterator.next();
    assertThat(s.getName(), equalTo("ModelAll"));
    assertThat(s.getNamespace(), equalTo("spannertest"));
    assertThat(s.getProp(GOOGLE_FORMAT_VERSION), equalTo("booleans"));
    assertThat(s.getProp(GOOGLE_STORAGE), equalTo("CloudSpanner"));
    assertThat(s.getProp(SPANNER_ENTITY), equalTo(SPANNER_ENTITY_MODEL));
    assertThat(s.getProp(SPANNER_REMOTE), equalTo("true"));
    assertThat(s.getProp(SPANNER_OPTION + "0"), equalTo("endpoint=\"test\""));
    assertThat(s.getFields(), hasSize(2));
    assertThat(s.getFields().get(0).name(), equalTo(INPUT));
    assertThat(s.getFields().get(0).schema().getType(), equalTo(Schema.Type.RECORD));
    assertThat(s.getFields().get(0).schema().getName(), equalTo("ModelAll_Input"));
    assertThat(s.getFields().get(0).schema().getFields(), hasSize(2));
    assertThat(s.getFields().get(0).schema().getFields().get(0).name(), equalTo("i1"));
    assertThat(
        s.getFields().get(0).schema().getFields().get(0).schema().getType(),
        equalTo(Schema.Type.BOOLEAN));
    assertThat(s.getFields().get(0).schema().getFields().get(0).getProp(SQL_TYPE), equalTo("BOOL"));
    assertThat(
        s.getFields().get(0).schema().getFields().get(0).getProp(SPANNER_OPTION + "0"),
        equalTo("required=FALSE"));
    assertThat(s.getFields().get(0).schema().getFields().get(1).name(), equalTo("i2"));
    assertThat(
        s.getFields().get(0).schema().getFields().get(1).schema().getType(),
        equalTo(Schema.Type.STRING));
    assertThat(
        s.getFields().get(0).schema().getFields().get(1).getProp(SQL_TYPE), equalTo("STRING(MAX)"));
    assertThat(s.getFields().get(1).name(), equalTo(OUTPUT));
    assertThat(s.getFields().get(1).schema().getType(), equalTo(Schema.Type.RECORD));
    assertThat(s.getFields().get(1).schema().getName(), equalTo("ModelAll_Output"));
    assertThat(s.getFields().get(1).schema().getFields(), hasSize(2));

    assertThat(s.getFields().get(1).schema().getFields().get(0).name(), equalTo("o1"));
    assertThat(
        s.getFields().get(1).schema().getFields().get(0).schema().getType(),
        equalTo(Schema.Type.LONG));
    assertThat(
        s.getFields().get(1).schema().getFields().get(0).getProp(SQL_TYPE), equalTo("INT64"));
    assertThat(
        s.getFields().get(1).schema().getFields().get(0).getProp(SPANNER_OPTION + "0"),
        equalTo("required=TRUE"));
    assertThat(s.getFields().get(1).schema().getFields().get(1).name(), equalTo("o2"));
    assertThat(
        s.getFields().get(1).schema().getFields().get(1).schema().getType(),
        equalTo(Schema.Type.DOUBLE));
    assertThat(
        s.getFields().get(1).schema().getFields().get(1).getProp(SQL_TYPE), equalTo("FLOAT64"));

    s = iterator.next();
    assertThat(s.getName(), equalTo("ModelMin"));
    assertThat(s.getNamespace(), equalTo("spannertest"));
    assertThat(s.getProp(GOOGLE_FORMAT_VERSION), equalTo("booleans"));
    assertThat(s.getProp(GOOGLE_STORAGE), equalTo("CloudSpanner"));
    assertThat(s.getProp(SPANNER_ENTITY), equalTo(SPANNER_ENTITY_MODEL));
    assertThat(s.getProp(SPANNER_REMOTE), equalTo("false"));
    assertThat(s.getFields(), hasSize(2));
    assertThat(s.getFields().get(0).name(), equalTo(INPUT));
    ;
    assertThat(s.getFields().get(0).schema().getType(), equalTo(Schema.Type.RECORD));
    assertThat(s.getFields().get(0).schema().getFields(), hasSize(1));
    assertThat(s.getFields().get(1).name(), equalTo(OUTPUT));
    assertThat(s.getFields().get(1).schema().getType(), equalTo(Schema.Type.RECORD));
    assertThat(s.getFields().get(0).schema().getFields(), hasSize(1));

    assertThat(iterator.hasNext(), equalTo(false));
  }

  @Test
  public void changeStreams() {
    DdlToAvroSchemaConverter converter =
        new DdlToAvroSchemaConverter("spannertest", "booleans", true);
    Ddl ddl =
        Ddl.builder()
            .createChangeStream("ChangeStreamAll")
            .forClause("FOR ALL")
            .options(
                ImmutableList.of(
                    "retention_period=\"7d\"", "value_capture_type=\"OLD_AND_NEW_VALUES\""))
            .endChangeStream()
            .createChangeStream("ChangeStreamEmpty")
            .endChangeStream()
            .createChangeStream("ChangeStreamTableColumns")
            .forClause("FOR `T1`, `T2`(`c1`, `c2`), `T3`()")
            .endChangeStream()
            .build();

    Collection<Schema> result = converter.convert(ddl);
    assertThat(result, hasSize(3));
    for (Schema s : result) {
      assertThat(s.getNamespace(), equalTo("spannertest"));
      assertThat(s.getProp(GOOGLE_FORMAT_VERSION), equalTo("booleans"));
      assertThat(s.getProp(GOOGLE_STORAGE), equalTo("CloudSpanner"));
      assertThat(s.getFields(), empty());
    }

    Iterator<Schema> it = result.iterator();
    Schema avroSchema1 = it.next();
    assertThat(avroSchema1.getName(), equalTo("ChangeStreamAll"));
    assertThat(avroSchema1.getProp(SPANNER_CHANGE_STREAM_FOR_CLAUSE), equalTo("FOR ALL"));
    assertThat(avroSchema1.getProp(SPANNER_OPTION + "0"), equalTo("retention_period=\"7d\""));
    assertThat(
        avroSchema1.getProp(SPANNER_OPTION + "1"),
        equalTo("value_capture_type=\"OLD_AND_NEW_VALUES\""));

    Schema avroSchema2 = it.next();
    assertThat(avroSchema2.getName(), equalTo("ChangeStreamEmpty"));
    assertThat(avroSchema2.getProp(SPANNER_CHANGE_STREAM_FOR_CLAUSE), equalTo(""));
    assertThat(avroSchema2.getProp(SPANNER_OPTION + "0"), nullValue());

    Schema avroSchema3 = it.next();
    assertThat(avroSchema3.getName(), equalTo("ChangeStreamTableColumns"));
    assertThat(
        avroSchema3.getProp(SPANNER_CHANGE_STREAM_FOR_CLAUSE),
        equalTo("FOR `T1`, `T2`(`c1`, `c2`), `T3`()"));
    assertThat(avroSchema3.getProp(SPANNER_OPTION + "0"), nullValue());
  }

  @Test
  public void pgChangeStreams() {
    DdlToAvroSchemaConverter converter =
        new DdlToAvroSchemaConverter("spannertest", "booleans", true);
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createChangeStream("ChangeStreamAll")
            .forClause("FOR ALL")
            .options(
                ImmutableList.of(
                    "retention_period='7d'", "value_capture_type='OLD_AND_NEW_VALUES'"))
            .endChangeStream()
            .createChangeStream("ChangeStreamEmpty")
            .endChangeStream()
            .createChangeStream("ChangeStreamTableColumns")
            .forClause("FOR \"T1\", \"T2\"(\"c1\", \"c2\"), \"T3\"()")
            .endChangeStream()
            .build();

    Collection<Schema> result = converter.convert(ddl);
    assertThat(result, hasSize(3));
    for (Schema s : result) {
      assertThat(s.getNamespace(), equalTo("spannertest"));
      assertThat(s.getProp(GOOGLE_FORMAT_VERSION), equalTo("booleans"));
      assertThat(s.getProp(GOOGLE_STORAGE), equalTo("CloudSpanner"));
      assertThat(s.getFields(), empty());
    }

    Iterator<Schema> it = result.iterator();
    Schema avroSchema1 = it.next();
    assertThat(avroSchema1.getName(), equalTo("ChangeStreamAll"));
    assertThat(avroSchema1.getProp(SPANNER_CHANGE_STREAM_FOR_CLAUSE), equalTo("FOR ALL"));
    assertThat(avroSchema1.getProp(SPANNER_OPTION + "0"), equalTo("retention_period='7d'"));
    assertThat(
        avroSchema1.getProp(SPANNER_OPTION + "1"),
        equalTo("value_capture_type='OLD_AND_NEW_VALUES'"));

    Schema avroSchema2 = it.next();
    assertThat(avroSchema2.getName(), equalTo("ChangeStreamEmpty"));
    assertThat(avroSchema2.getProp(SPANNER_CHANGE_STREAM_FOR_CLAUSE), equalTo(""));
    assertThat(avroSchema2.getProp(SPANNER_OPTION + "0"), nullValue());

    Schema avroSchema3 = it.next();
    assertThat(avroSchema3.getName(), equalTo("ChangeStreamTableColumns"));
    assertThat(
        avroSchema3.getProp(SPANNER_CHANGE_STREAM_FOR_CLAUSE),
        equalTo("FOR \"T1\", \"T2\"(\"c1\", \"c2\"), \"T3\"()"));
    assertThat(avroSchema3.getProp(SPANNER_OPTION + "0"), nullValue());
  }

  @Test
  public void sequences() {
    DdlToAvroSchemaConverter converter =
        new DdlToAvroSchemaConverter("spannertest", "booleans", true);
    Ddl ddl =
        Ddl.builder()
            .createSequence("Sequence1")
            .options(
                ImmutableList.of(
                    "sequence_kind=\"bit_reversed_positive\"",
                    "skip_range_min=0",
                    "skip_range_max=1000",
                    "start_with_counter=50"))
            .endSequence()
            .createSequence("Sequence2")
            .options(
                ImmutableList.of(
                    "sequence_kind=\"bit_reversed_positive\"", "start_with_counter=9999"))
            .endSequence()
            .createSequence("Sequence3")
            .options(ImmutableList.of("sequence_kind=\"bit_reversed_positive\""))
            .endSequence()
            .build();

    Collection<Schema> result = converter.convert(ddl);
    assertThat(result, hasSize(3));
    for (Schema s : result) {
      assertThat(s.getNamespace(), equalTo("spannertest"));
      assertThat(s.getProp("googleFormatVersion"), equalTo("booleans"));
      assertThat(s.getProp("googleStorage"), equalTo("CloudSpanner"));
      assertThat(s.getFields(), empty());
    }

    Iterator<Schema> it = result.iterator();
    Schema avroSchema1 = it.next();
    assertThat(avroSchema1.getName(), equalTo("Sequence1"));
    assertThat(
        avroSchema1.getProp("sequenceOption_0"),
        equalTo("sequence_kind=\"bit_reversed_positive\""));
    assertThat(avroSchema1.getProp("sequenceOption_1"), equalTo("skip_range_min=0"));
    assertThat(avroSchema1.getProp("sequenceOption_2"), equalTo("skip_range_max=1000"));
    assertThat(avroSchema1.getProp("sequenceOption_3"), equalTo("start_with_counter=50"));

    Schema avroSchema2 = it.next();
    assertThat(avroSchema2.getName(), equalTo("Sequence2"));
    assertThat(
        avroSchema2.getProp("sequenceOption_0"),
        equalTo("sequence_kind=\"bit_reversed_positive\""));
    assertThat(avroSchema2.getProp("sequenceOption_1"), equalTo("start_with_counter=9999"));

    Schema avroSchema3 = it.next();
    assertThat(avroSchema3.getName(), equalTo("Sequence3"));
    assertThat(
        avroSchema3.getProp("sequenceOption_0"),
        equalTo("sequence_kind=\"bit_reversed_positive\""));
  }

  @Test
  public void pgSequences() {
    DdlToAvroSchemaConverter converter =
        new DdlToAvroSchemaConverter("spannertest", "booleans", true);
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createSequence("PGSequence1")
            .sequenceKind("bit_reversed_positive")
            .counterStartValue(Long.valueOf(50))
            .skipRangeMin(Long.valueOf(0))
            .skipRangeMax(Long.valueOf(1000))
            .endSequence()
            .createSequence("PGSequence2")
            .sequenceKind("bit_reversed_positive")
            .counterStartValue(Long.valueOf(9999))
            .endSequence()
            .createSequence("PGSequence3")
            .sequenceKind("bit_reversed_positive")
            .endSequence()
            .build();

    Collection<Schema> result = converter.convert(ddl);
    assertThat(result, hasSize(3));
    for (Schema s : result) {
      assertThat(s.getNamespace(), equalTo("spannertest"));
      assertThat(s.getProp("googleFormatVersion"), equalTo("booleans"));
      assertThat(s.getProp("googleStorage"), equalTo("CloudSpanner"));
      assertThat(s.getFields(), empty());
    }

    Iterator<Schema> it = result.iterator();
    Schema avroSchema1 = it.next();
    assertThat(avroSchema1.getName(), equalTo("PGSequence1"));
    assertThat(avroSchema1.getProp(SPANNER_SEQUENCE_KIND), equalTo("bit_reversed_positive"));
    assertThat(avroSchema1.getProp(SPANNER_SEQUENCE_SKIP_RANGE_MIN), equalTo("0"));
    assertThat(avroSchema1.getProp(SPANNER_SEQUENCE_SKIP_RANGE_MAX), equalTo("1000"));
    assertThat(avroSchema1.getProp(SPANNER_SEQUENCE_COUNTER_START), equalTo("50"));

    Schema avroSchema2 = it.next();
    assertThat(avroSchema2.getName(), equalTo("PGSequence2"));
    assertThat(avroSchema2.getProp(SPANNER_SEQUENCE_KIND), equalTo("bit_reversed_positive"));
    assertThat(avroSchema2.getProp(SPANNER_SEQUENCE_COUNTER_START), equalTo("9999"));

    Schema avroSchema3 = it.next();
    assertThat(avroSchema3.getName(), equalTo("PGSequence3"));
    assertThat(avroSchema3.getProp(SPANNER_SEQUENCE_KIND), equalTo("bit_reversed_positive"));
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

  private Schema nullablePgNumericArray() {
    return Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        Schema.createArray(
            Schema.createUnion(
                Schema.create(Schema.Type.NULL),
                LogicalTypes.decimal(NumericUtils.PG_MAX_PRECISION, NumericUtils.PG_MAX_SCALE)
                    .addToSchema(Schema.create(Schema.Type.BYTES)))));
  }

  private Schema nullablePgNumericUnion() {
    return Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        LogicalTypes.decimal(NumericUtils.PG_MAX_PRECISION, NumericUtils.PG_MAX_SCALE)
            .addToSchema(Schema.create(Schema.Type.BYTES)));
  }
}
