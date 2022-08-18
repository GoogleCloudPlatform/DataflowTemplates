package com.google.cloud.teleport.it.bigtable;

import com.google.cloud.bigtable.admin.v2.models.StorageType;

import java.io.IOException;

public class BigtableResourceManagerCluster {

    private final String clusterId;

    private final String region;

    private final int numNodes;

    private final StorageType storageType;

    BigtableResourceManagerCluster(BigtableResourceManagerCluster.Builder builder) {
        this(
                builder.clusterId,
                builder.region,
                builder.numNodes,
                builder.storageType
        );
    }
    BigtableResourceManagerCluster(String clusterId, String region, int numNodes, StorageType storageType) {
        this.clusterId = clusterId;
        this.region = region;
        this.numNodes = numNodes;
        this.storageType = storageType;
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getRegion() {
        return region;
    }

    public int getNumNodes() {
        return numNodes;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public static BigtableResourceManagerCluster.Builder builder(String clusterId, String region, int numNodes, StorageType storageType) {
        return new BigtableResourceManagerCluster.Builder(clusterId, region, numNodes, storageType);
    }

    /** Builder for {@link BigtableResourceManagerCluster}. */
    public static final class Builder {

        private final String clusterId;

        private final String region;

        private final int numNodes;

        private final StorageType storageType;

        private Builder(String clusterId, String region, int numNodes, StorageType storageType) {
            this.clusterId = clusterId;
            this.region = region;
            this.numNodes = numNodes;
            this.storageType = storageType;
        }

        public BigtableResourceManagerCluster build() throws IOException {
            return new BigtableResourceManagerCluster(this);
        }
    }
}
