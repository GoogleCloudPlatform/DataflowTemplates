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
package com.google.cloud.teleport.v2.spanner.migrations.utils;

import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;

/**
 * Class containing util methods that use the {@link com.google.cloud.teleport.v2.spanner.migrations.schema.Schema} object.
 */
public class SchemaUtils {

    /**
     * Verify if given table name is valid in the session file.
     */
    public static void verifyTableInSession(Schema schema, String tableName)
            throws IllegalArgumentException, DroppedTableException {
        if (!schema.getSrcToID().containsKey(tableName)) {
            throw new IllegalArgumentException(
                    "Missing entry for " + tableName + " in srcToId map, provide a valid session file.");
        }
        if (!schema.getToSpanner().containsKey(tableName)) {
            throw new DroppedTableException(
                    "Cannot find entry for "
                            + tableName
                            + " in toSpanner map, it is likely this table was dropped");
        }
        String tableId = schema.getSrcToID().get(tableName).getName();
        if (!schema.getSpSchema().containsKey(tableId)) {
            throw new IllegalArgumentException(
                    "Missing entry for " + tableId + " in spSchema, provide a valid session file.");
        }
    }
}
