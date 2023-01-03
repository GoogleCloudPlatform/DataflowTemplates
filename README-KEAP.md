# Keap DataFlow Fork Modifications

## Testing and Deploying

Recent changes to the main branch have broken made building the project a multi-step process.

From the project root:
```bash
mvn install
```

Some modules may fail to build but so long as the parent project, the metadata and the integration-test modules build successfully you can proceed to build the template and deploy it as below. 

### Integration

```bash
gcloud config set project is-events-dataflow-intg

cd v1

mvn compile exec:java \
-Dexec.mainClass=com.infusionsoft.dataflow.templates.PubsubToBigQuery \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=is-events-dataflow-intg \
--region=us-central1 \
--workerZone=us-central1-c \
--stagingLocation=gs://is-events-dataflow-intg/staging \
--tempLocation=gs://is-events-dataflow-intg/temp \
--templateLocation=gs://is-events-dataflow-intg/templates/pubsub2BGQ.json \
--runner=DataflowRunner \
--defaultWorkerLogLevel=ERROR"

gcloud dataflow jobs run pubsub-to-bigquery \
--gcs-location=gs://is-events-dataflow-intg/templates/pubsub2BGQ.json \
--region=us-central1 \
--zone=us-central1-c \
--service-account-email=is-events-dataflow-intg@is-events-dataflow-intg.iam.gserviceaccount.com \
--parameters=inputTopic=projects/is-flagship-events-intg/topics/v1.segment-events-core
```

Shut down the job after testing [via Cloud Console](https://console.cloud.google.com/dataflow/jobs?authuser=1&project=is-events-dataflow-intg)

-----

### Staging

Update the parameter in `PubsubToBigQuery.java` to point to Stge
> L157 from `is-events-dataflow-intg` to `is-events-dataflow-stge`

```bash
gcloud config set project is-events-dataflow-stge

cd v1

mvn compile exec:java \
-Dexec.mainClass=com.infusionsoft.dataflow.templates.PubsubToBigQuery \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=is-events-dataflow-stge \
--region=us-central1 \
--workerZone=us-central1-c \
--stagingLocation=gs://is-events-dataflow-stge/staging \
--tempLocation=gs://is-events-dataflow-stge/temp \
--templateLocation=gs://is-events-dataflow-stge/templates/pubsub2BGQ.json \
--runner=DataflowRunner \
--defaultWorkerLogLevel=ERROR"

gcloud dataflow jobs run pubsub-to-bigquery \
--gcs-location=gs://is-events-dataflow-stge/templates/pubsub2BGQ.json \
--region=us-central1 \
--zone=us-central1-c \
--service-account-email=is-events-dataflow-stge@is-events-dataflow-stge.iam.gserviceaccount.com \
--parameters=inputTopic=projects/is-flagship-events-stge/topics/v1.segment-events-core
```

Shut down the job after testing [via Cloud Console](https://console.cloud.google.com/dataflow/jobs?authuser=1&project=is-events-dataflow-stge)

-----

### Production

Update the parameter in `PubsubToBigQuery.java` to point to Prod
> L157 from `is-events-dataflow-intg` to `is-events-dataflow-prod`

```bash
gcloud config set project is-events-dataflow-prod

cd v1

mvn compile exec:java \
-Dexec.mainClass=com.infusionsoft.dataflow.templates.PubsubToBigQuery \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=is-events-dataflow-prod \
--region=us-central1 \
--workerZone=us-central1-c \
--stagingLocation=gs://is-events-dataflow-prod/staging \
--tempLocation=gs://is-events-dataflow-prod/temp \
--templateLocation=gs://is-events-dataflow-prod/templates/pubsub2BGQ.json \
--runner=DataflowRunner \
--defaultWorkerLogLevel=ERROR"
```

Version the jobname by incrementing the name:

```bash
gcloud dataflow jobs run pubsub-to-bigquery-v8 \
--gcs-location=gs://is-events-dataflow-prod/templates/pubsub2BGQ.json \
--region=us-central1 \
--zone=us-central1-c \
--service-account-email=is-events-dataflow-prod@is-events-dataflow-prod.iam.gserviceaccount.com \
--parameters=inputTopic=projects/is-flagship-events-prod/topics/v1.segment-events-core
```
Shutdown the existing job (using "Drain" option) [via Cloud Console](https://console.cloud.google.com/dataflow/jobs?authuser=1&project=is-events-dataflow-prod)
