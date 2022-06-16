/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Type;
import java.io.Serializable;

/**
 * The {@link TrackedSpannerColumn} contains the name, type and ordinal position of a Spanner column
 * tracked by a change stream.
 */
@AutoValue
public abstract class TrackedSpannerColumn implements Serializable {

  public static TrackedSpannerColumn create(String name, Type type, int ordinalPosition) {
    return new AutoValue_TrackedSpannerColumn(name, type, ordinalPosition);
  }

  public abstract String getName();

  public abstract Type getType();

  public abstract int getOrdinalPosition();
}
