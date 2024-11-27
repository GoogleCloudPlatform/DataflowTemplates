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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

import com.google.cloud.teleport.v2.neo4j.model.sources.TextFormat;
import org.apache.commons.csv.CSVFormat;

public class CsvSources {
  public static CSVFormat toCsvFormat(TextFormat format) {
    switch (format) {
      case EXCEL:
        return CSVFormat.EXCEL.withNullString("");
      case INFORMIX:
        return CSVFormat.INFORMIX_UNLOAD_CSV.withNullString("");
      case MONGO:
        return CSVFormat.MONGODB_CSV.withNullString("");
      case MONGO_TSV:
        return CSVFormat.MONGODB_TSV.withNullString("");
      case MYSQL:
        return CSVFormat.MYSQL.withNullString("");
      case ORACLE:
        return CSVFormat.ORACLE.withNullString("");
      case POSTGRES:
        return CSVFormat.POSTGRESQL_TEXT.withNullString("");
      case POSTGRESQL_CSV:
        return CSVFormat.POSTGRESQL_CSV.withNullString("");
      case RFC4180:
        return CSVFormat.RFC4180.withNullString("");
      case DEFAULT:
      default:
        return CSVFormat.DEFAULT.withNullString("");
    }
  }
}
