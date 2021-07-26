package com.google.cloud.teleport.v2.elasticsearch.utils;

import java.util.Arrays;

/**
 * IndexWrapper builds index based on parameters dataset and namespace.
 * **/
public class IndexWrapper {
    private final Dataset dataset;
    private final String namespace;

    public IndexWrapper(String dataset, String namespace) {
        this.dataset = Arrays.stream(Dataset.values())
                .filter(value -> value.toString().equalsIgnoreCase(dataset))
                .findFirst()
                .orElse(Dataset.PUBSUB);

        this.namespace = namespace;
    }

    public IndexWrapper(Dataset dataset, String namespace) {
        this.dataset = dataset;
        this.namespace = namespace;
    }

    @Override
    public String toString() {
        return "logs-gcp." + this.dataset.toString().toLowerCase() + "-" + this.namespace;
    }

    /**
     * Enum of possible dataset values.
     */
    public enum Dataset {
        AUDIT,
        VPCFLOW,
        FIREWALL,
        PUBSUB
    }
}
