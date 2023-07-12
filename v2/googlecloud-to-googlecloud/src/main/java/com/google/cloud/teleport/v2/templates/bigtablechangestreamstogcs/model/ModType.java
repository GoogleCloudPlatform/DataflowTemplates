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

import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.BigtableUtils;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * {@link ModType} represents the type of Modification that CDC {@link
 * com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation} entries represent.
 */
public enum ModType {
  SET_CELL("SET_CELL"),
  DELETE_FAMILY("DELETE_FAMILY"),
  DELETE_CELLS("DELETE_CELLS"),
  UNKNOWN("UNKNOWN");

  ModType(String propertyName) {
    this.propertyName = propertyName;
  }

  private String propertyName;

  public String getPropertyName() {
    return this.propertyName;
  }

  public ByteBuffer getPropertyNameAsByteBuffer(Charset charset) {
    return BigtableUtils.copyByteBuffer(ByteBuffer.wrap(this.propertyName.getBytes(charset)));
  }
}
