package com.google.cloud.teleport.v2.elasticsearch.utils;

import java.util.Arrays;

/**
 * IndexWrapper builds index based on parameters dataset and namespace.
 * **/
public class IndexWrapper {
    private final DATASET dataset;
    private final String namespace;

    public IndexWrapper(String dataset, String namespace) {
        this.dataset = Arrays.stream(DATASET.values())
                .filter(value -> value.toString().equalsIgnoreCase(dataset))
                .findFirst()
                .orElse(DATASET.PUBSUB);

        this.namespace = namespace;
    }

    @Override
    public String toString() {
        return "logs-gcp." + this.dataset.toString().toLowerCase() + "-" + this.namespace;
    }

    private enum DATASET {
        AUDIT,
        VPCFLOW,
        FIREWALL,
        PUBSUB
    }
}
