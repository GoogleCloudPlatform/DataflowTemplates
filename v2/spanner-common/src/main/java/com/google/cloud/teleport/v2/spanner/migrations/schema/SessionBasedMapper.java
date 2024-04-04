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

import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class SessionBasedMapper implements SchemaMapperIface {

    Ddl ddl;

    Schema schema;

    public SessionBasedMapper(Schema schema, Ddl ddl) {
        this.schema = schema;
        this.ddl = ddl;
    }


    @Override
    public String getSpannerTableName(String srcTable) throws NoSuchElementException {
        Map<String, NameAndCols> toSpanner = schema.getToSpanner();
        if (!toSpanner.containsKey(srcTable)) {
            throw new NoSuchElementException(String.format("Source table %s not found", srcTable));
        }
        return toSpanner.get(srcTable).getName();
    }

    @Override
    public String getSpannerColumnName(String srcTable, String srcColumn) throws NoSuchElementException {
        Map<String, NameAndCols> toSpanner = schema.getToSpanner();
        if (!toSpanner.containsKey(srcTable)) {
            throw new NoSuchElementException(String.format("Source table %s not found", srcTable));
        }
        Map<String, String> cols = toSpanner.get(srcTable).getCols();
        if (!cols.containsKey(srcColumn)) {
            throw new NoSuchElementException(String.format("Source column %s not found for table %s", srcColumn, srcTable));
        }
        return cols.get(srcColumn);
    }

    @Override
    public String getSourceColumnName(String spannerTable, String spannerColumn) throws NoSuchElementException {
        Map<String, NameAndCols> toSource = schema.getToSource();
        if (!toSource.containsKey(spannerTable)) {
            throw new NoSuchElementException(String.format("Spanner table %s not found", spannerTable));
        }
        Map<String, String> cols = toSource.get(spannerTable).getCols();
        if (!cols.containsKey(spannerColumn)) {
            throw new NoSuchElementException(String.format("Spanner column %s not found for table %s", spannerColumn, spannerTable));
        }
        return cols.get(spannerColumn);
    }

    @Override
    public Type getSpannerColumnType(String spannerTable, String spannerColumn) throws NoSuchElementException {
        Table spTable = ddl.table(spannerTable);
        if (spTable == null) {
            throw new NoSuchElementException(String.format("Spanner table %s not found", spannerTable));
        }
        Column col = spTable.column(spannerColumn);
        if (col == null) {
            throw new NoSuchElementException(String.format("Spanner column %s not found", spannerColumn));
        }
        return col.type();
    }

    @Override
    public List<String> getSpannerColumns(String spannerTable) throws NoSuchElementException {
        Map<String, NameAndCols> toSource = schema.getToSource();
        if (!toSource.containsKey(spannerTable)) {
            throw new NoSuchElementException(String.format("Spanner table %s not found", spannerTable));
        }
        Map<String, String> spToSrcCols = toSource.get(spannerTable).getCols();
        return new ArrayList<>(spToSrcCols.keySet());
    }
}
