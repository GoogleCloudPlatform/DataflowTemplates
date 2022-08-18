package com.google.cloud.teleport.it.bigtable;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.collect.ImmutableList;

import java.io.IOException;

public class BigtableResourceManagerTest {
    public static void main(String[] args) throws IOException {

        // Create Bigtable Resource Manager
        DefaultBigtableResourceManager rm = new DefaultBigtableResourceManager("sample-test", "jeff-test-project-templates");

//        BigtableResourceManagerCluster cluster = new BigtableResourceManagerCluster("cluster-2", "us-central1-a", 1, StorageType.SSD);
//        Iterable<BigtableResourceManagerCluster> clusters = ImmutableList.of(cluster);
//        rm.createInstance(clusters);

        // Create table
        Iterable<String> columnFamilies = ImmutableList.of("names");
        rm.createTable("table1", columnFamilies);

        // Write rows to table
        RowMutation rowMutation = RowMutation.create("table1", "rowKey" + 1)
                .setCell("names", "first", "John")
                .setCell("names", "last", "Smith");
        rm.write(rowMutation);

        // Read and print rows from table
        ImmutableList<Row> rows = rm.readTable("table1");
        for(Row row : rows) {
            System.out.println(row);
        }

        // Remove all resources
//        rm.cleanupAll();
    }
}
