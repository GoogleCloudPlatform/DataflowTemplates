/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.datastream;

import static org.junit.Assert.assertEquals;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class ShadowTableReadUtilsTest {
  private Ddl ddl;
  private static final String SHADOW_TABLE = "shadow_test_table";
  private static final List<String> READ_COLUMNS =
      Arrays.asList("bool_field", "string_field", "version");

  @Before
  public void setUp() {
    ddl =
        Ddl.builder()
            .createTable("shadow_test_table")
            // Primary key columns - one of each type
            .column("bool_field")
            .bool()
            .endColumn()
            .column("int64_field")
            .int64()
            .endColumn()
            .column("float64_field")
            .float64()
            .endColumn()
            .column("numeric_field")
            .numeric()
            .endColumn()
            .column("string_field")
            .string()
            .max()
            .endColumn()
            .column("bytes_field")
            .bytes()
            .max()
            .endColumn() // BYTES
            .column("timestamp_field")
            .timestamp()
            .endColumn() // TIMESTAMP
            .column("date_field")
            .date()
            .endColumn() // DATE
            // Additional non-PK columns
            .column("version")
            .int64()
            .endColumn()
            .column("extra_field")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("bool_field")
            .asc("int64_field")
            .asc("float64_field")
            .asc("numeric_field")
            .asc("string_field")
            .asc("bytes_field")
            .asc("timestamp_field")
            .asc("date_field")
            .end()
            .endTable()
            .build();
  }

  @Test
  public void testGenerateShadowTableReadSQL_AllTypes() {
    boolean boolValue = true;
    long int64Value = 123L;
    double float64Value = 123.45;
    BigDecimal numericValue = new BigDecimal("123.456");
    String stringValue = "test_string";
    ByteArray bytesValue = ByteArray.copyFrom("test_bytes");
    Timestamp timestampValue = Timestamp.ofTimeMicroseconds(1234567);
    Date dateValue = Date.fromYearMonthDay(2024, 1, 1);

    Key primaryKey =
        Key.of(
            boolValue, // BOOL
            int64Value, // INT64
            float64Value, // FLOAT64
            numericValue, // NUMERIC
            stringValue, // STRING
            bytesValue, // BYTES
            timestampValue, // TIMESTAMP
            dateValue // DATE
            );

    Statement stmt =
        ShadowTableReadUtils.generateShadowTableReadSQL(
            SHADOW_TABLE, READ_COLUMNS, primaryKey, ddl);

    String expectedSql =
        "@{LOCK_SCANNED_RANGES=exclusive} SELECT bool_field, string_field, version "
            + "FROM shadow_test_table WHERE bool_field=@bool_field AND int64_field=@int64_field AND "
            + "float64_field=@float64_field AND numeric_field=@numeric_field AND "
            + "string_field=@string_field AND bytes_field=@bytes_field AND "
            + "timestamp_field=@timestamp_field AND date_field=@date_field";

    assertEquals(expectedSql, stmt.getSql());

    Map<String, Value> params = stmt.getParameters();

    assertEquals(Value.bool(boolValue), params.get("bool_field"));
    assertEquals(Value.int64(int64Value), params.get("int64_field"));
    assertEquals(Value.float64(float64Value), params.get("float64_field"));
    assertEquals(Value.numeric(numericValue), params.get("numeric_field"));
    assertEquals(Value.string(stringValue), params.get("string_field"));
    assertEquals(Value.bytes(bytesValue), params.get("bytes_field"));
    assertEquals(Value.timestamp(timestampValue), params.get("timestamp_field"));
    assertEquals(Value.date(dateValue), params.get("date_field"));
  }
}
