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
package com.google.cloud.teleport.it.bigtable;

import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.common.collect.ImmutableList;

/**
 * Utilities for {@link com.google.cloud.teleport.it.bigtable.BigtableResourceManager}
 * implementations.
 */
public final class BigtableResourceManagerUtils {

    private BigtableResourceManagerUtils() {}
    
    static Iterable<BigtableResourceManagerCluster> generateDefaultClusters(String instanceId, String zone, int numNodes, StorageType storageType) {
        String clusterId = instanceId.substring(0, instanceId.length() - 3) + "-c1";
        BigtableResourceManagerCluster cluster = new BigtableResourceManagerCluster(clusterId, zone, numNodes, storageType);
        return ImmutableList.of(cluster);
    }
}
