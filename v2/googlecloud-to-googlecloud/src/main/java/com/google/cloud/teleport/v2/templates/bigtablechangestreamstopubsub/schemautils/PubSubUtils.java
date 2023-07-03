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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.schemautils;

import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.BigtableSource;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.PubSubDestination;
import java.io.Serializable;

/**
 * {@link PubSubUtils} provides utils for processing BigQuery schema and generating BigQuery rows.
 */
public class PubSubUtils implements Serializable {

  private final BigtableSource source;
  private final PubSubDestination destination;
  private final String pubSubAPI;

  public PubSubUtils(
      BigtableSource sourceInfo, PubSubDestination destinationInfo, String pubSubAPI) {
    this.source = sourceInfo;
    this.destination = destinationInfo;
    this.pubSubAPI = pubSubAPI;
  }

  public BigtableSource getSource() {
    return source;
  }

  public PubSubDestination getDestination() {
    return destination;
  }

  public String getPubSubAPI() {
    return pubSubAPI;
  }
}
