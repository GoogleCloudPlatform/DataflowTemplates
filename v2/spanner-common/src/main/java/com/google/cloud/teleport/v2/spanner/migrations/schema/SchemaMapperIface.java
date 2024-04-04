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
package com.google.cloud.teleport.v2.spanner.migrations.schema;

import com.google.cloud.teleport.v2.spanner.type.Type;

import java.util.List;
import java.util.NoSuchElementException;

public interface SchemaMapperIface {
    /**
     * Retrieves the corresponding Spanner table name given a source table name.
     */
    String getSpannerTableName(String srcTable) throws NoSuchElementException;

    /**
     * Retrieves the corresponding Spanner column name given a source table and source column.
     */
    String getSpannerColumnName(String srcTable, String srcColumn) throws NoSuchElementException;

    /**
     * Retrieves the corresponding source column name given a Spanner table and Spanner column.
     */
    String getSourceColumnName(String spannerTable, String spannerColumn) throws NoSuchElementException;

    /**
     * Retrieves the Spanner column data type given a spanner table and spanner column.
     */
    Type getSpannerColumnType(String spannerTable, String spannerColumn) throws NoSuchElementException;

    /**
     * Retrieves a list of all column names within a Spanner table.
     */
    List<String> getSpannerColumns(String spannerTable) throws NoSuchElementException;
}
