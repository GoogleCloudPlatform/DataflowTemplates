/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.v2.datastream.utils;

import com.google.cloud.teleport.v2.utils.MappedObjectCache;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DataStreamPkCache} stores an expiring cached list of PKs for each stream, schema, and
 * table combination.
 */
public class DataStreamPkCache extends MappedObjectCache<List<String>, List<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamPkCache.class);

  public DataStreamClient client;

  public DataStreamPkCache(DataStreamClient client) {
    this.client = client;
  }

  @Override
  public List<String> getObjectValue(List<String> key) {
    try {
      return this.client.getPrimaryKeys(key.get(0), key.get(1), key.get(2));
    } catch (IOException e) {
      LOG.error("IOException: DataStream Discovery on Primary Keys Failed: {}", e.toString());
      return null;
    }
  }
}
