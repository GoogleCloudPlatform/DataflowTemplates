/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model;

import java.io.Serializable;

public class DestinationInfo implements Serializable {

  private final String bigtableChangeStreamCharset;
  private final boolean useBase64Rowkey;
  private final boolean useBase64ColumnQualifier;
  private final boolean useBase64Value;

  public DestinationInfo(
      String bigtableChangeStreamCharset,
      boolean useBase64Rowkey,
      boolean useBase64ColumnQualifier,
      boolean useBase64Value) {

    this.bigtableChangeStreamCharset = bigtableChangeStreamCharset;
    this.useBase64Rowkey = useBase64Rowkey;
    this.useBase64ColumnQualifier = useBase64ColumnQualifier;
    this.useBase64Value = useBase64Value;
  }

  public String getCharsetName() {
    return bigtableChangeStreamCharset;
  }

  public boolean isColumnQualifierBase64Encoded() {
    return useBase64ColumnQualifier;
  }

  public boolean isValueBase64Encoded() {
    return useBase64Value;
  }

  public boolean isRowkeyBase64Encoded() {
    return useBase64Rowkey;
  }
}
