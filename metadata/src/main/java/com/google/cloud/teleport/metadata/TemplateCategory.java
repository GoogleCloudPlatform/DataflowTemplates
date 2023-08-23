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
package com.google.cloud.teleport.metadata;

/** Category that can be used on Dataflow Templates. */
public enum TemplateCategory {
  GET_STARTED(1, "get_started", "Get Started"),

  STREAMING(2, "STREAMING", "Process Data Continuously (stream)"),

  BATCH(3, "BATCH", "Process Data in Bulk (batch)"),

  UTILITIES(4, "utilities", "Utilities"),

  LEGACY(5, "legacy", "Legacy Templates");

  public static final TemplateCategory[] ORDERED = {
    GET_STARTED, STREAMING, BATCH, UTILITIES, LEGACY
  };

  final int order;
  final String name;
  final String displayName;

  TemplateCategory(int order, String name, String displayName) {
    this.order = order;
    this.name = name;
    this.displayName = displayName;
  }

  public int getOrder() {
    return order;
  }

  public String getName() {
    return name;
  }

  public String getDisplayName() {
    return displayName;
  }
}
