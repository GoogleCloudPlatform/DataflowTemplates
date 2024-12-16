/*
 * Copyright (C) 2024 Google LLC
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
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomTransformationWithShardForLiveIT implements ISpannerMigrationTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(CustomShardIdFetcher.class);

  @Override
  public void init(String parameters) {
    LOG.info("init called with {}", parameters);
  }

  @Override
  public MigrationTransformationResponse toSpannerRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    if (request.getTableName().equals("Customers")) {
      Map<String, Object> row = new HashMap<>(request.getRequestRow());
      row.put("full_name", row.get("first_name") + " " + row.get("last_name"));
      row.put("migration_shard_id", request.getShardId() + "_" + row.get("id"));
      MigrationTransformationResponse response = new MigrationTransformationResponse(row, false);
      return response;
    } else if (request.getTableName().equals("AllDatatypeTransformation")) {
      Map<String, Object> row = new HashMap<>(request.getRequestRow());
      // Filter event in case "varchar_column" = "example1"
      if (row.get("varchar_column").equals("example1")) {
        return new MigrationTransformationResponse(null, true);
      }
      // In case of update events, return request as response without any transformation
      if (request.getEventType().equals("UPDATE-INSERT")) {
        return new MigrationTransformationResponse(null, false);
      }
      // In case of backfill update the values for all the columns in all the rows except the
      // filtered row.
      row.put("tinyint_column", (Long) row.get("tinyint_column") + 1);
      row.put("text_column", row.get("text_column") + " append");
      row.put("int_column", (Long) row.get("int_column") + 1);
      row.put("bigint_column", (Long) row.get("bigint_column") + 1);
      row.put("float_column", (double) row.get("float_column") + 1);
      row.put("double_column", (double) row.get("double_column") + 1);
      Double value = Double.parseDouble((String) row.get("decimal_column"));
      row.put("decimal_column", String.valueOf(value + 1));
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
      LocalTime time = LocalTime.parse((String) row.get("time_column"), formatter);

      // Add one hour to the time
      LocalTime newTime = time.plusHours(1);
      row.put("time_column", newTime.format(formatter));
      row.put("bool_column", 1);
      row.put("enum_column", "1");
      row.put("blob_column", "576f726d64");
      row.put("binary_column", "0102030405060708090A0B0C0D0E0F1011121314");
      row.put("bit_column", 13);
      row.put("year_column", (Long) row.get("year_column") + 1);
      try {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
        Date date = dateFormat.parse((String) row.get("date_column"));
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        row.put("date_column", dateFormat.format(calendar.getTime()));
        Date dateTime = dateTimeFormat.parse((String) row.get("datetime_column"));
        calendar.setTime(dateTime);
        calendar.add(Calendar.SECOND, -1);
        row.put("datetime_column", dateTimeFormat.format(calendar.getTime()));
        dateTime = dateTimeFormat.parse((String) row.get("timestamp_column"));
        calendar.setTime(dateTime);
        calendar.add(Calendar.SECOND, -1);
        row.put("timestamp_column", dateTimeFormat.format(calendar.getTime()));

      } catch (Exception e) {
        throw new InvalidTransformationException(e);
      }
      MigrationTransformationResponse response = new MigrationTransformationResponse(row, false);
      return response;
    }
    return new MigrationTransformationResponse(null, false);
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
      Long bigIntColumn = Long.parseLong((String) requestRow.get("bigint_column")) + 1;
      Long yearColumn = Long.parseLong((String) requestRow.get("year_column")) + 1;
      BigDecimal floatColumn = (BigDecimal) requestRow.get("float_column");
      BigDecimal doubleColumn = (BigDecimal) requestRow.get("double_column");
      responseRow.put("tinyint_column", tinyIntColumn.toString());
      responseRow.put("text_column", "\'" + requestRow.get("text_column") + " append\'");
      responseRow.put("int_column", intColumn.toString());
      responseRow.put("bigint_column", bigIntColumn.toString());
      responseRow.put("float_column", floatColumn.add(BigDecimal.ONE).toString());
      responseRow.put("double_column", doubleColumn.add(BigDecimal.ONE).toString());
      Double value = Double.parseDouble((String) requestRow.get("decimal_column"));
      responseRow.put("decimal_column", String.valueOf(value - 1));
      responseRow.put("bool_column", "false");
      responseRow.put("enum_column", "\'3\'");
      responseRow.put(
          "blob_column",
          "from_base64(\'"
              + Base64.getEncoder()
                  .encodeToString("blob_column_appended".getBytes(StandardCharsets.UTF_8))
              + "\')");
      responseRow.put(
          "binary_column",
          "binary(from_base64(\'"
              + Base64.getEncoder()
                  .encodeToString("binary_column_appended".getBytes(StandardCharsets.UTF_8))
              + "\'))");
      responseRow.put(
          "bit_column",
          "binary(from_base64(\'"
              + Base64.getEncoder().encodeToString("5".getBytes(StandardCharsets.UTF_8))
              + "\'))");
      responseRow.put("year_column", "\'" + yearColumn + "\'");
      try {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
        dateTimeFormat.setTimeZone(TimeZone.getTimeZone("UTC")); // Ensure it handles UTC correctly
        Date date = dateFormat.parse((String) requestRow.get("date_column"));
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        responseRow.put("date_column", "\'" + dateFormat.format(calendar.getTime()) + "\'");
        Date dateTime = dateTimeFormat.parse((String) requestRow.get("datetime_column"));
        calendar.setTime(dateTime);
        calendar.add(Calendar.SECOND, -1);
        String dateTimeColumn = dateTimeFormat.format(calendar.getTime());
        responseRow.put(
            "datetime_column",
            "CONVERT_TZ(\'"
                + dateTimeColumn.substring(0, dateTimeColumn.length() - 1)
                + "\','+00:00','+00:00')");
        dateTime = dateTimeFormat.parse((String) requestRow.get("timestamp_column"));
        calendar.setTime(dateTime);
        calendar.add(Calendar.SECOND, -1);
        String timestampColumn = dateTimeFormat.format(calendar.getTime());
        responseRow.put(
            "timestamp_column",
            "CONVERT_TZ(\'"
                + timestampColumn.substring(0, timestampColumn.length() - 1)
                + "\','+00:00','+00:00')");
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
}
