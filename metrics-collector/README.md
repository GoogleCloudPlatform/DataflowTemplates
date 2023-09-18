## Overview
> Command line tool to fetch the dataflow job metrics, estimates job cost and stores the results in the output store (i.e bigquery or file system)

## Commands

Supports multiple functionalities through following command types:

| Command Type         | Description                                          |
|----------------------|:-----------------------------------------------------|
| COLLECT_METRICS      | For complete details refer [here](#CollectMetrics)   |
| LAUNCH_AND_COLLECT   | For complete details refer [here](#LaunchCollect)    |

## Output Stores
User can choose appropriate output store based on the command line options: output_type and output_location:

| Output Type | Output Location                                              |
|-------------|:-------------------------------------------------------------|
| BIGQUERY    | project-id.dataset.table                                     |
| FILE        | /path/to/location <br/>(Supports GCS and Local File Systems) |

In order to store the metrics in bigquery, create the table using schema specified in 
```scripts/metrics_bigquery_schema.json```.  

Sample commands for creating dataset and table are provided in ```scripts/bigquery.json```

## Getting Started

### Requirements

* Java 11
* Maven 3
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

- Set following environment variables
```bash
$ export GOOGLE_APPLICATION_CREDENTIALS=</path/to/application_default_credentials.json>
```

- Execute below command to format the code from the project root directory:
```bash
$ mvn -pl metrics-collector spotless:apply
```

- To build the fat jar, execute the below command from within the project root directory which will generate
  ```{project_dir}/metrics-collector/target/metrics-collector-1.0.jar```

```bash
$ mvn -pl metrics-collector clean package
```

### BIGQUERY OUTPUT STORE</a>
Bigquery can be used to store the metrics collected by the tool.  In order to store the metrics create the table
in bigquery using schema specified 

### <a name="CollectMetrics">COLLECT_METRICS</a>
Collects the metrics for a given job id and stores the results in the output store.

### Creating the configuration File
The configuration file provides project_id, region and job_id along with pricing (optional) of the job for
which the metrics needs to be collected

#### Example configuration File
Below is an example configuration file. pricing section is optional. If pricing is skipped then estimated cost of the
job will not be calculated.

```json
{
  "project": "test-project",
  "region": "us-central1",
  "jobId": "2023-09-05_12_40_15-5693029202890139182",
  "pricing": {
    "vcpuPerHour": 0.056,
    "memoryGbPerHour": 0.003557,
    "pdGbPerHour": 0.000054,
    "ssdGbPerHour": 10,
    "shuffleDataPerGb": 0.011,
    "streamingDataPerGb": 0.005
  }
}
```

### Execute the command
```bash
# To store results in BigQuery
$ java -jar /path/to/metrics-collector-1.0.jar --command COLLECT_METRICS --conf /path/to/config.json \
--output_type=BIGQUERY --output_location=projectid:datasetid.tableid

# To store results in GCS / local file system
$ java -jar /path/to/metrics-collector-1.0.jar --command COLLECT_METRICS --conf /path/to/config.json \
--output_type=FILE --output_location=/path/to/output/location
```

### Sample Output
```json
{
   "run_timestamp": "2023-09-18T22:50:43.419455Z",
   "pipeline_name": "word-count-benchmark-20230908224412968",
   "job_create_timestamp": "2023-09-08T22:44:14.475764Z",
   "job_type": "JOB_TYPE_BATCH",
   "sdk_version": "2.49.0",
   "sdk": "Apache Beam SDK for Java",
   "metrics": {
      "TotalDpuUsage": 0.050666635590718774,
      "TotalSeCuUsage": 0.0,
      "TotalStreamingDataProcessed": 0.0,
      "TotalDcuUsage": 50666.63559071875,
      "BillableShuffleDataProcessed": 1.603737473487854E-5,
      "TotalPdUsage": 2279.0,
      "TotalGpuTime": 0.0,
      "EstimatedJobCost": 0.007,
      "TotalVcpuTime": 182.0,
      "TotalShuffleDataProcessed": 6.414949893951416E-5,
      "TotalMemoryUsage": 747068.0,
      "TotalSsdUsage": 0.0
   }
}
```

### <a name="LaunchCollect">LAUNCH_AND_COLLECT</a>
Launches either Dataflow [classic](https://cloud.google.com/dataflow/docs/guides/templates/creating-templates)
or [flex](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates) template and waits for the job
to finish or time out (in case of streaming), collects the metrics and stores the results to Output Store

* To cancel streaming jobs provide the appropriate timeout using timeoutInMinutes property in supplied config value.
  Refer config file below for an example.

### Creating the configuration File
The configuration file provides project_id, template name, template type, template spec and pipeline options

#### Example configuration File
Below is an example configuration file with minimal run time environment options.

```json
{
  "project": "test-project",
  "region": "us-central1",
  "templateName": "SampleWordCount",
  "templateVersion": "1.0",
  "templateType": "classic",
  "templateSpec": "gs://dataflow-templates/latest/Word_Count",
  "jobPrefix": "WordCountBenchmark",
  "timeoutInMinutes" : 30,
  "pipelineOptions": {
    "inputFile": "gs://dataflow-samples/shakespeare/kinglear.txt",
    "output": "gs://bucket-name/output/wordcount/"
  },
  "environmentOptions": {
    "tempLocation": "gs://bucket-name/temp/"
  }
}
```
Below is another example configuration file with additional run time environment options and pricing details.

```json
{
  "project": "test-project",
  "region": "us-central1",
  "templateName": "SampleWordCount",
  "templateVersion": "1.0",
  "templateType": "classic",
  "templateSpec": "gs://dataflow-templates/latest/Word_Count",
  "jobPrefix": "WordCountBenchmark",
  "timeoutInMinutes" : 30,
  "pipelineOptions": {
    "inputFile": "gs://dataflow-samples/shakespeare/kinglear.txt",
    "output": "gs://bucket-name/output/wordcount/"
  },
  "environmentOptions": {
    "numWorkers" : 2,
    "maxWorkers" : 10,
    "service_account_email" : "my-svc-acct@project-id.iam.gserviceaccount.com",
    "zone" : "us-central1-b",
    "autoscalingAlgorithm" : "AUTOSCALING_ALGORITHM_BASIC",
    "subnetwork": "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/default",
    "tempLocation": "gs://bucket-name/temp/",
    "ipConfiguration": "WORKER_IP_PRIVATE",
    "additionalExperiments": ["enable_prime"],
    "additionalUserLabels": {
      "topology":"pointtopoint",
      "dataformats":"text",
      "datasizecategory":"medium",
      "datasizeingb":"100"
    }
  },
  "pricing": {
    "vcpuPerHour": 0.056,
    "memoryGbPerHour": 0.003557,
    "pdGbPerHour": 0.000054,
    "ssdGbPerHour": 10,
    "shuffleDataPerGb": 0.011,
    "streamingDataPerGb": 0.005
  }
}
```
For complete list of run time environment options for various template types check below commands:
```bash
# For classic templates
$ gcloud dataflow jobs run --help

# For flex templates
$ gcloud dataflow flex-template run --help
```

### Execute the command
```bash
# To store results in BigQuery
$ java -jar /path/to/metrics-collector-1.0.jar --command LAUNCH_AND_COLLECT --conf /path/to/config.json \
--output_type=BIGQUERY --output_location=projectid:datasetid.tableid

# To store results in GCS / local file system
$ java -jar /path/to/metrics-collector-1.0.jar --command LAUNCH_AND_COLLECT --conf /path/to/config.json \
--output_type=FILE --output_location=/path/to/output/location
```

### Sample Output
```json
{
  "run_timestamp": "2023-09-18T22:50:43.419455Z",
  "pipeline_name": "word-count-benchmark-20230908224412968",
  "job_create_timestamp": "2023-09-08T22:44:14.475764Z",
  "job_type": "JOB_TYPE_BATCH",
  "template_name": "SampleWordCount",
  "template_version": "1.0",
  "sdk_version": "2.49.0",
  "template_type": "classic",
  "sdk": "Apache Beam SDK for Java",
  "metrics": {
    "TotalDpuUsage": 0.050666635590718774,
    "TotalSeCuUsage": 0.0,
    "TotalStreamingDataProcessed": 0.0,
    "TotalDcuUsage": 50666.63559071875,
    "BillableShuffleDataProcessed": 1.603737473487854E-5,
    "TotalPdUsage": 2279.0,
    "TotalGpuTime": 0.0,
    "EstimatedJobCost": 0.045,
    "TotalVcpuTime": 182.0,
    "TotalShuffleDataProcessed": 6.414949893951416E-5,
    "TotalMemoryUsage": 747068.0,
    "TotalSsdUsage": 0.0
  },
  "parameters": {
    "output": "gs://bucket-name/output/wordcount/",
    "tempLocation": "gs://bucket-name/temp/",
    "additionalExperiments": "[enable_prime, workerMachineType=n1-standard-2, minNumWorkers=2]",
    "inputFile": "gs://dataflow-samples/shakespeare/kinglear.txt",
    "maxWorkers": "10",
    "additionalUserLabels": "{sources=gcs, sinks=bigtable, topology=pointtopoint, dataformats=text, datasizecategory=medium, datasizeingb=100}",
    "subnetwork": "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/default",
    "numWorkers": "2"
  }
}
```

## Output Metrics

| MetricName                   | Metric Description                                                                                      |
|------------------------------|:--------------------------------------------------------------------------------------------------------|
| TotalVcpuTime                | The total vCPU seconds used by Dataflow job                                                             |
| TotalGpuTime                 | The total proportion of time in which the GPU was used by Dataflow job                                  |
| TotalMemoryUsage             | The total GB seconds of memory allocated to Dataflow job                                                |
| TotalPdUsage                 | The total GB seconds for all persistent disk used by all workers associated with Dataflow job           |
| TotalSsdUsage                | The total GB seconds for all SSD used by all workers associated with Dataflow job                       |
| TotalShuffleDataProcessed    | The total bytes of shuffle data processed by Dataflow job                                               |
| TotalStreamingDataProcessed  | The total bytes of streaming data processed by Dataflow job                                             |
| BillableShuffleDataProcessed | The billable bytes of shuffle data processed by Dataflow job                                            |
| TotalDcuUsage                | The total amount of DCUs (Data Compute Unit) used by the Dataflow job since it was launched.            |
| TotalElapsedTimeSec          | Total duration of the pipeline that job is in RUNNING_STATE in seconds                                  |
| EstimatedJobCost             | Estimated cost of the dataflow job (if pricing info is provided). Not applicable for prime enabled jobs |

For complete list of metrics refer [Dataflow Monitoring Metrics](https://cloud.google.com/monitoring/api/metrics_gcp#gcp-dataflow)

## Cost Estimation

Job cost can be estimated by providing the resource pricing information inside the config file as shown below:

```json
{
   "project": "test-project",
   "region": "us-central1",
   "templateName": "SampleWordCount",
   "pipelineOptions": {},
   "environmentOptions": {},
   "pricing": {
      "vcpuPerHour": 0.056,
      "memoryGbPerHour": 0.003557,
      "pdGbPerHour": 0.000054,
      "ssdGbPerHour": 10,
      "shuffleDataPerGb": 0.011,
      "streamingDataPerGb": 0.005
   }
}
```
Pricing for various resource by region can be obtained from [Dataflow Pricing](https://cloud.google.com/dataflow/pricing) docs.

Note:
* Estimated cost is appropriate and doesn't factor any discounts provided.
* For prime enabled jobs, estimated cost feature is not supported and remains 0.0
