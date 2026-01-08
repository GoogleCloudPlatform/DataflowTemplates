package org.apache.beam.it.gcp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestConstants {
  public static final Map<String, List<String>> SPANNER_TEST_BUCKETS =
      new HashMap<>() {
        {
          put(
              "cloud-teleport-testing",
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
                  "cloud-teleport-spanner-it-9"));
          put(
              "span-cloud-teleport-testing",
              List.of(
                  "span-cloud-teleport-testing-it-0",
                  "span-cloud-teleport-testing-it-1",
                  "span-cloud-teleport-testing-it-2",
                  "span-cloud-teleport-testing-it-3",
                  "span-cloud-teleport-testing-it-4",
                  "span-cloud-teleport-testing-it-5",
                  "span-cloud-teleport-testing-it-6",
                  "span-cloud-teleport-testing-it-7",
                  "span-cloud-teleport-testing-it-8",
                  "span-cloud-teleport-testing-it-9"));
        }
      };

  public static final List<String> SPANNER_TEST_INSTANCES =
      List.of("teleport", "teleport1", "teleport2", "teleport3", "teleport4");
}
