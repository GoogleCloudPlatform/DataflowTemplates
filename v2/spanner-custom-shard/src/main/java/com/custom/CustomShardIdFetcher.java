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
package com.custom;

import com.google.cloud.teleport.v2.spanner.utils.IShardIdFetcher;
import com.google.cloud.teleport.v2.spanner.utils.ShardIdRequest;
import com.google.cloud.teleport.v2.spanner.utils.ShardIdResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a sample class to be implemented by the customer. All the relevant dependencies have been
 * added and users need to implement the getShardId() method
 */
public class CustomShardIdFetcher implements IShardIdFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(CustomShardIdFetcher.class);

  @Override
  public ShardIdResponse getShardId(ShardIdRequest shardIdRequest) {
    LOG.info("Returning custom sharding function");
    return new ShardIdResponse();
  }
}
