# Running Integration Tests Locally with DirectRunner

## 1. What is DirectRunner and How It Works

The `DirectRunner` executes Beam pipelines entirely on your local machine within a single JVM, rather than submitting the job to the Google Cloud Dataflow backend. It simulates the behavior of a distributed runner by keeping `PCollection` data in local memory and executing transforms across local threads.

### A. When to Use DirectRunner (Benefits)
- **Fast Iteration:** Pipeline execution begins immediately on your local machine, completely bypassing the 3-5 minute Cloud Dataflow VM provisioning time.
- **Unit & Integration Testing:** Perfect for validating local logic changes in a fast feedback loop before pushing code.

### B. When NOT to Use DirectRunner
- **Performance/Load Testing:** `DirectRunner` is limited by your local machine's CPU and memory. It cannot dynamically scale or distribute work across multiple workers. Always use `DataflowRunner` for volume testing.
- **Dynamic Sharding on Unbounded PCollections:** `DirectRunner` lacks native support for dynamic file sharding on unbounded streams. The codebase contains specific `if-else` workarounds for this, but complex sharding logic should be tested in the cloud.
- **Streaming Pipeline Checkpoints:** `DirectRunner` does not replicate Dataflow's exact stateful checkpointing and watermark mechanics perfectly. If you are debugging subtle watermark delays or state timers, `DataflowRunner` is the source of truth.

---

## 2. Prerequisites

Before running the tests locally, ensure your environment is configured properly:

1. **GCP Credentials:** The test framework interacts with real Google Cloud resources (like ephemeral Spanner databases and GCS buckets) to validate the pipeline, so set the required GCP credentials.
   ```bash
   gcloud auth application-default login
   gcloud config set project <your-gcp-project>
   ```
2. **Docker:** Ensure Docker is running on your machine, as Testcontainers will often be used to spin up local databases (e.g., Oracle, MySQL, Postgres).
3. **Static Resources (Live ITs):** For certain "live" integration tests, the framework does not use Testcontainers and instead expects static resources. Ensure your local machine has the required source systems running and bound to the specific ports expected by the test framework.

---

## 3. Running the Tests

To instruct the IT framework to use the local `DirectRunner` instead of launching a Cloud Dataflow job, pass the `-DdirectRunnerTest=true` system property to your Maven command.

**Standard Command:**
```bash
mvn clean verify -pl v2/<your-module-name> -PtemplatesIntegrationTests -Dtest=<your-test-class-name> -DdirectRunnerTest=true
```

### Example:
When actively debugging a failing test locally, you can export necessary static resource identifiers as environment variables and aggressively skip Maven's slow formatting (Spotless/Checkstyle) and container (Jib) checks to compile and run the test in seconds.

First, set your environment variables:
```bash
export TEST_ARTIFACT_BUCKET="<your-gcs-artifact-bucket>"
export TEST_STAGE_BUCKET="<your-gcs-stage-bucket>"
export TEST_PROJECT="<your-gcp-project-id>"
export TEST_REGION="<your-gcp-region>"
export TEST_SPANNER_INSTANCE="<your-spanner-instance-id>"
export TEST_HOST_IP="<your-source-db-host-ip>"
export TEST_PRIVATE_CONNECTIVITY="<your-datastream-private-connectivity-id>"
export TEST_CLOUD_PROXY_IP="<your-cloud-sql-proxy-ip>"
export TEST_CLOUD_USERNAME="<your-cloud-sql-username>"
export TEST_CLOUD_PASSWORD="<your-cloud-sql-password>"
```

Then, you can run the integration tests using this highly optimized Maven command:

```bash
mvn clean verify -f pom.xml -U -PtemplatesIntegrationTests -pl v2/<your-module-name> -am -Dtest=<your-test-class-name> -DdirectRunnerTest=true -Dsurefire.useFile=false -e -Dmdep.analyze.skip -Dcheckstyle.skip -Dspotless.check.skip=true -Djib.skip -DskipShade -Dsurefire.failIfNoSpecifiedTests=false -DartifactBucket=${TEST_ARTIFACT_BUCKET} -DstageBucket=${TEST_STAGE_BUCKET} -Dproject=${TEST_PROJECT} -Dregion=${TEST_REGION} -DspannerInstanceId=${TEST_SPANNER_INSTANCE} -DhostIp=${TEST_HOST_IP}
```

*(Note: If you are running a live migration test such as `datastream-to-spanner`, append `-DprivateConnectivity=${TEST_PRIVATE_CONNECTIVITY} -DcloudProxyHost=${TEST_CLOUD_PROXY_IP} -DcloudProxyUsername=${TEST_CLOUD_USERNAME} -DcloudProxyPassword=${TEST_CLOUD_PASSWORD}` to the command above).*
