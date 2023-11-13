package org.apache.beam.it.gcp.bigtable;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import org.apache.beam.it.common.TestProperties;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.beam.it.common.utils.ResourceManagerUtils.generateResourceId;

public class test {
  public static void main(String[] args) throws IOException {

    Credentials credentials = TestProperties.buildCredentialsFromEnv();
    CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(credentials);
    BigtableResourceManager rm = BigtableResourceManager
        .builder("test-id", "cloud-teleport-testing", credentialsProvider)
//        .setInstanceId("nokill-jkinard-test")
//        .useStaticInstance()
        .build();

    String clusterId = "nokill-jkinard-test";
    rm.createInstance(List.of(BigtableResourceManagerCluster.create(clusterId, "us-central1-a", 1, StorageType.HDD)));
    rm.createTable("read_table", List.of("cf1"));
    rm.createTable("write_table", List.of("cf1"));

    List<RowMutation> rows = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      rows.add(RowMutation.create("read_table", "row_" + i)
          .setCell("cf1", "col1", i)
          .setCell("cf1", "col2", "a" + i));
    }
    rm.write(rows);
    System.out.println();
    rm.cleanupAll();
  }
}
