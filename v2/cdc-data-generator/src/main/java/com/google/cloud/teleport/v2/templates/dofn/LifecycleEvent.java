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
package com.google.cloud.teleport.v2.templates.dofn;

import java.io.Serializable;
import java.util.LinkedHashMap;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.values.Row;

/**
 * A lifecycle event (UPDATE or DELETE) scheduled against a previously-inserted row.
 *
 * <p>The primary key is carried as an ordered map of {@code (columnName -> value)} so that both
 * composite and non-integer PKs round-trip correctly. The map is a {@link LinkedHashMap} so
 * iteration order matches the declared PK column order.
 */
@DefaultCoder(SerializableCoder.class)
public class LifecycleEvent implements Serializable {

  private static final long serialVersionUID = 1L;

  public LinkedHashMap<String, Object> pkValues;
  public String type;
  public String tableName;
  public Row reducedRow;

  public LifecycleEvent(
      LinkedHashMap<String, Object> pkValues, String type, String tableName, Row reducedRow) {
    this.pkValues = pkValues;
    this.type = type;
    this.tableName = tableName;
    this.reducedRow = reducedRow;
  }
}
