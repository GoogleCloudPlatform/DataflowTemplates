/*
 * Copyright (C) 2026 Google LLC
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
package com.custom;

import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomTransformationWithOracleForIT implements ISpannerMigrationTransformer {

  private static final Logger LOG =
      LoggerFactory.getLogger(CustomTransformationWithOracleForIT.class);

  @Override
  public void init(String parameters) {
    LOG.info("init called with {}", parameters);
  }

  @Override
  public MigrationTransformationResponse toSpannerRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    return new CustomTransformationWithShardForLiveIT().toSpannerRow(request);
  }

  private String bytesToHex(byte[] bytes) {
    StringBuilder hexString = new StringBuilder();
    for (byte b : bytes) {
      String hex = Integer.toHexString(0xFF & b);
      if (hex.length() == 1) {
        hexString.append('0');
      }
      hexString.append(hex);
    }
    return hexString.toString().toUpperCase();
  }

  @Override
  public MigrationTransformationResponse toSourceRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    if (request.getTableName().equals("AllDatatypeTransformation")) {
      Map<String, Object> responseRow = new HashMap<>();
      Map<String, Object> requestRow = request.getRequestRow();
      // Filter event in case "varchar_column" = "example1"
      if (requestRow.get("varchar_column").equals("example1")) {
        return new MigrationTransformationResponse(null, true);
      }
      // In case of update/delete events, return request as response without any transformation
      if (request.getEventType().equals("UPDATE")) {
        return new MigrationTransformationResponse(null, false);
      }
      if (request.getEventType().equals("DELETE")) {
        return new MigrationTransformationResponse(null, true);
      }
      // In case of INSERT update the values for all the columns in all the rows except the
      // filtered row.
      Long tinyIntColumn = Long.parseLong((String) requestRow.get("tinyint_column")) + 1;
      Long intColumn = Long.parseLong((String) requestRow.get("int_column")) + 1;
      Long sourceOnlyPk = intColumn - tinyIntColumn;
      Long bigIntColumn = Long.parseLong((String) requestRow.get("bigint_column")) + 1;
      Long yearColumn = Long.parseLong((String) requestRow.get("year_column")) + 1;
      BigDecimal floatColumn = (BigDecimal) requestRow.get("float_column");
      BigDecimal doubleColumn = (BigDecimal) requestRow.get("double_column");
      responseRow.put("source_only_pk", sourceOnlyPk.toString());
      responseRow.put("tinyint_column", tinyIntColumn.toString());
      responseRow.put("text_column", "\'" + requestRow.get("text_column") + " append\'");
      responseRow.put("int_column", intColumn.toString());
      responseRow.put("bigint_column", bigIntColumn.toString());
      responseRow.put("float_column", floatColumn.add(BigDecimal.ONE).toString());
      responseRow.put("double_column", doubleColumn.add(BigDecimal.ONE).toString());
      Double value = Double.parseDouble((String) requestRow.get("decimal_column"));
      responseRow.put("decimal_column", String.valueOf(value - 1));
      responseRow.put("bool_column", "0"); // Oracle uses 1/0 usually, mapped from false
      responseRow.put("enum_column", "\'3\'");

      // Oracle HEXTORAW formatting
      responseRow.put(
          "blob_column",
          "HEXTORAW('"
              + bytesToHex("blob_column_appended".getBytes(StandardCharsets.UTF_8))
              + "')");

      responseRow.put(
          "binary_column",
          "HEXTORAW('"
              + bytesToHex("binary_column_appended".getBytes(StandardCharsets.UTF_8))
              + "')");

      responseRow.put(
          "bit_column", "HEXTORAW('" + bytesToHex("5".getBytes(StandardCharsets.UTF_8)) + "')");

      responseRow.put("year_column", "\'" + yearColumn + "\'");
      try {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
        dateTimeFormat.setTimeZone(TimeZone.getTimeZone("UTC")); // Ensure it handles UTC correctly
        Date date = dateFormat.parse((String) requestRow.get("date_column"));
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        responseRow.put(
            "date_column",
            "TO_DATE('" + dateFormat.format(calendar.getTime()) + "', 'YYYY-MM-DD')");

        Date dateTime = dateTimeFormat.parse((String) requestRow.get("datetime_column"));
        calendar.setTime(dateTime);
        calendar.add(Calendar.SECOND, -1);
        String dateTimeColumn = dateTimeFormat.format(calendar.getTime());
        // Oracle TO_TIMESTAMP
        responseRow.put(
            "datetime_column",
            "CAST(FROM_TZ(CAST(TO_TIMESTAMP('"
                + dateTimeColumn.substring(0, dateTimeColumn.length() - 1)
                + "', 'YYYY-MM-DD\"T\"HH24:MI:SS') AS TIMESTAMP), 'UTC') AT TIME ZONE '+00:00' AS TIMESTAMP)");

        dateTime = dateTimeFormat.parse((String) requestRow.get("timestamp_column"));
        calendar.setTime(dateTime);
        calendar.add(Calendar.SECOND, -1);
        String timestampColumn = dateTimeFormat.format(calendar.getTime());
        responseRow.put(
            "timestamp_column",
            "CAST(FROM_TZ(CAST(TO_TIMESTAMP('"
                + timestampColumn.substring(0, timestampColumn.length() - 1)
                + "', 'YYYY-MM-DD\"T\"HH24:MI:SS') AS TIMESTAMP), 'UTC') AT TIME ZONE '+00:00' AS TIMESTAMP)");

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        LocalTime time = LocalTime.parse((String) requestRow.get("time_column"), formatter);

        LocalTime newTime = time.plusMinutes(10);
        responseRow.put("time_column", "\'" + newTime.format(formatter) + "\'");

      } catch (Exception e) {
        throw new InvalidTransformationException(e);
      }

      MigrationTransformationResponse response =
          new MigrationTransformationResponse(responseRow, false);
      return response;
    } else if (request.getTableName().equals("Person")) {
      Map<String, Object> responseRow = new HashMap<>();
      Map<String, Object> requestRow = request.getRequestRow();
      String firstName1 = requestRow.get("first_name1").toString();
      String lastName1 = requestRow.get("last_name1").toString();
      String firstName2 = requestRow.get("first_name2").toString();
      String lastName2 = requestRow.get("last_name2").toString();
      String firstName3 = requestRow.get("first_name3").toString();
      String lastName3 = requestRow.get("last_name3").toString();
      responseRow.put("full_name1", "\'" + firstName1 + " " + lastName1 + "\'");
      responseRow.put("full_name2", "\'" + firstName2 + " " + lastName2 + "\'");
      responseRow.put("full_name3", "\'" + firstName3 + " " + lastName3 + "\'");
      MigrationTransformationResponse response =
          new MigrationTransformationResponse(responseRow, false);
      return response;
    } else if (request.getTableName().equals("Users1")) {
      Map<String, Object> responseRow = new HashMap<>();
      Map<String, Object> requestRow = request.getRequestRow();
      String name = requestRow.get("name").toString();
      String[] nameArray = name.split(" ");
      responseRow.put("first_name", "\'" + nameArray[0] + "\'");
      responseRow.put("last_name", "\'" + nameArray[1] + "\'");
      MigrationTransformationResponse response =
          new MigrationTransformationResponse(responseRow, false);
      return response;
    }
    return new MigrationTransformationResponse(null, false);
  }

  @Override
  public MigrationTransformationResponse transformFailedSpannerMutation(
      MigrationTransformationRequest request) throws InvalidTransformationException {
    return new MigrationTransformationResponse(null, false);
  }
}
