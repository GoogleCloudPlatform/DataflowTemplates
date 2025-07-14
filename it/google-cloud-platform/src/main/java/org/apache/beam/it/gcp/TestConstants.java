package org.apache.beam.it.gcp;

import java.util.List;

public class TestConstants {
  public static final List<String> SPANNER_TEST_BUCKETS =
      List.of(
          "cloud-teleport-spanner-it-0",
          "cloud-teleport-spanner-it-1",
          "cloud-teleport-spanner-it-2",
          "cloud-teleport-spanner-it-3",
          "cloud-teleport-spanner-it-4",
          "cloud-teleport-spanner-it-5",
          "cloud-teleport-spanner-it-6",
          "cloud-teleport-spanner-it-7",
          "cloud-teleport-spanner-it-8",
          "cloud-teleport-spanner-it-9");

  public static final List<String> SPANNER_TEST_INSTANCES =
      List.of("teleport", "teleport1", "teleport2", "teleport3", "teleport4");
}
