# Dataflow CDC Example

This directory contains components for a Change-data Capture (CDC) solution to
capture data from an MySQL database, and sync it into BigQuery. The solution
relies on Cloud Dataflow, and [Debezium](https://debezium.io/), and excellent
open source project for change data capture.

To implement the CDC solution in this repository:

1. Deploy a [Debezium](https://debezium.io/) embedded connector for MySQL
2. Start a Dataflow pipeline that syncs MySQL and BigQuery tables

The embedded connector connects to MySQL, and tracks the binary change
log. Whenever a new change occurs, it formats it into a Beam `Row` and
pushes it into a PubSub topic.

The Dataflow pipeline watches on a PubSub topic for each table that
you would want to sync from MySQL to BigQuery. It then it pushes those
updates to BigQuery tables which are periodically synchronized, thus
having a replica table in BigQuery from your MySQL database.

Note the [currently unsupported scenarios](#unsupported-scenarios) for
this solution.

## Requirements

- Java 8
- Maven
- A MySQL instance
- A PubSub topic for each MySQL table that you would like to synchronize with BigQuery.

## Getting Started

This section outlines how to deploy the whole solution. The first step to get
started is to deploy the Debezium embedded connector. You can deploy the
connector in the following ways:

- Directly from the source
- As a Docker container
- On Kubernetes via GKE

Once the connector is deployed, and publishing data to PubSub, you can start the
Dataflow pipeline.

### Deploying the Connector

The deployment of this solution involves deploying the two main components:

 (1) The Dataflow pipeline just needs to be run on a GCP project, with access to the
appropriate PubSub topic, and BigQuery datasets.

(2) The Debezium embedded connector can be deployed in a few different ways: On
a VM by executing the JAR, or as a Docker container; or on your Kubernetes cluster,
be it on GKE, or on-premise.

The connector can be deployed locally from source, via a docker container,
or with high-reliability on Kubernetes. Before deploying the connector, make
sure to have set up the PubSub topics and subscriptions for it.
See [Setting up PubSub topics](#setting-up-pubsub-topics).

It will need to be supplied of two/three
basic configuration files:

- A properties file containing:
  - The name of the database: `databaseName=...`
  - A username to access the database changelog: `databaseUsername=...`
  - An IP address or DNS to connect to the database: `databaseAddress=...`
  - **Optionally** a port to connect to the database: `databasePort=...` (default: 3306)
  - A project in Google Cloud Platform where the PubSub topic lives: `gcpProject=...`
  - A prefix for PubSub topics corresponding to each MySQL table.
     The connector will push table updates
     to `${PREFIX}${DB_INSTANCE}.${DATABASE}.${TABLE}`: `gcpPubsubTopicPrefix=...`.
  - A comma-separated list of whitelisted tables: `whitelistedTables=...`
    - These tables should have their names fully qualified `${instance}.${database}.${table}`.
    - E.g.: `whitelistedTables=prodmysql.storedatabase.products,prodmysql.storedatabase.customers`.
  - **Optionally** a password for the database: `databasePassword=...`
  - **Optionally** an indicator for the connector to store state in memory, or in persistent disk: `inMemoryOffsetStorage=[true/false]`.
    - This parameter is used when deploying the connector for testing vs production.
    - **If you are deploying the connector in production**, you will set this parameter to false,
      and provide a persistent storage medium for it to store state.
    - **If you are deploying the connector locally for testing**, you can set this parameter to true,
      and state will be lost on restarts.
- An **optional properties file** containing the database password: `databasePassword=...`
  - This extra file can be passed as a secret for Kubernetes deployment, without revealing
    the password in the main properties file.
- A credentials file with privileges to push to Cloud PubSub, and update Entries in
  Google Cloud Catalog in the `GOOGLE_DEFAULT_CREDENTIALS` environment variable.

#### Deploying from source

If you would like to deploy the connector from your machine after cloning this
repository, you can run it easily with the following command:

```
mvn exec:java -pl cdc-embedded-connector \
    -Dexec.args="path/to/your/properties/file.properties [path/to/password/file.properties]"
```

Deploying in this manner will rely on your machine's Google Cloud credentials.

#### Deploying as a Docker Container from Source

To deploy the connector as a docker container is a middle step from deploying
a resilient connector on a cluster. This means that the configuration needs to
be fully provided when starting up the container.

```
mvn compile -pl cdc-embedded-connector jib:dockerBuild

docker run \
  -v path/to/properties/file.properties:/etc/dataflow-cdc/dataflow_cdc.properties \
  -v path/to/properties/password/file.properties:/etc/dataflow-cdc/dataflow_cdc_password.properties \
  -v path/to/json/gcp/credentials.json:/etc/gcp_credentials.json \
  -e "GOOGLE_APPLICATION_CREDENTIALS=/etc/gcp_credentials.json"  \
  dataflow-cdc-connector
```

#### Deploying on Kubernetes via GKE

To have a full deployment of the connector so that it will recover upon failures,
and restart from already-published offsets, and run continuously, you will
want to deploy it in a cluster. The deployment in a cluster involves the
following rough steps:

1. Set up networking for connecting to Cloud SQL from GKE
1. Create a Persistent Volume for your container to store committed offsets
1. Build, and push the container
1. Create a cluster
1. Create configurations in kubectl (e.g. properties, passwords)
1. Deploying the container

#### Setting up Cloud SQL network connectivity

An important detail is that to connect to a Cloud SQL instance from GKE
you will need to do it via a Private IP address, or with a Cloud SQL proxy
container.
[Check out GCP documentation on how to set this up](https://cloud.google.com/sql/docs/mysql/connect-kubernetes-engine).
First you can create a basic cluster on GKE, which will run our connector pushing
updates from MySQL to PubSub.

We assume that:

- You have a GKE cluster, we will call it `cdc-connector-cluster`
- If you want to store your docker images in GCR, you have already configured
  `gcloud` to be a credential helper for docker (e.g. via `gcloud auth configure-docker`).

##### Create a Persistent Disk to attach to your container

First you can create the disk. Make sure that it's in the same zone as your GKE cluster.

```
gcloud compute disks create offset-tracker-pv --size=10Gi --zone ${GCP_ZONE}
```

##### Build and push the container

You can build the container locally using `mvn compile -pl cdc-embedded-connector jib:dockerBuild`. Once
you've done that, you will want to push it to a Docker image repository where
you can pull it:

```
docker tag cdc-embedded-connector ${REPOSITORY}/${GCP_PROJECT}/cdc-embedded-connector
docker push ${REPOSITORY}/${GCP_PROJECT}/dataflow-cdc-connector
```

Once you have pushed it, please **replace the `${REPOSITORY}` and
`${GCP_PROJECT}` tags from the file `cdc-embedded-connector/app.yml`**.

##### Creating configuration

To pass configuration files to the connector, you will want to declare configmaps, and
secrets with all of them.

Any information that is sensitive, such as passwords, or GCP credentials should
be created as a secret in k8s. For instance, the JSON key to push to GCP:

```
kubectl create secret generic pubsub-key --from-file=key.json=LOCAL-PATH-TO-KEY-FILE.json
```

The properties file can be converted to a ConfigMap, which is the recommended
way of passing non-sensitive configuration information:
```
kubectl create configmap cdc-connector-props --from_file=dataflow_cdc.properties=LOCAL-PATH-TO-PROPERTIES-FILE.json
```

##### Deploying the connector

After setting up all of these configurations, we're ready to deploy the connector using
Kubernetes:

```
kubectl apply -f app.yml
```

### Setting up PubSub topics

Let's suppose you have a MySQL database running in any environment.
In this case, we’ll consider a database running on Cloud SQL, with two tables:
`people` and `pets`. So we have:

- Instance name: my-mysql
- Database name: cdc_demo
- Tables
  - `people` — fully qualified name is `my-mysql.cdc_demo.people`
  - `pets` — fully qualified name is `my-mysql.cdc_demo.pets`

The Debezium connector exports data for each table into a separate Pub/Sub topic
with a prefix. We’ll choose this prefix for our Pub/Sub topics: `export_demo_`.
This prefix will be passed as an argument to the Debezium connector, along with
a Google Cloud project. The PubSub topics that we'll create are:

- Table: `my-mysql.cdc_demo.people`
  - Topic: `export_demo_my-mysql_cdc_demo_people`
  - Subscription: `cdc_demo_people_subscription`
- Table: `my-mysql.cdc_demo.pets`
  - Topic: `export_demo_my-mysql_cdc_demo_pets`
  - Subscription: `cdc_demo_pets_subscription`

You can then pass this prefix to the Debezium connector via properties
`gcpPubsubTopicPrefix=export_demo_`, and the subscriptions to the Dataflow
pipeline as Pipeline Options 
`--inputSubscriptions=cdc_demo_people_subscription,cdc_demo_pets_subscription`.


## The Dataflow Pipeline

The Dataflow pipeline for this CDC solution is meant to be started **after
the Debezium connector has started**. This will allow the connector to
append schemas to Cloud Data Catalog, and these schemas to be used for the
pipeline.

### Running the Pipeline

To deploy the pipeline from source, you can run the following command:

```
mvn exec:java -pl cdc-change-applier -Dexec.args="--runner=DataflowRunner \
              --inputSubscriptions=${PUBSUB_SUBSCRIPTIONS} \
              --updateFrequencySecs=300 \
              --changeLogDataset=${CHANGELOG_BQ_DATASET} \
              --replicaDataset=${REPLICA_BQ_DATASET} \
              --project=${GCP_PROJECT}"
```

## Unsupported scenarios

This solution does not support a few particular scenarios:

- **Updates to Primary Keys**: In its first version, this solution does not
  support changes to the Primary Key of any of the rows.
- **Schema changes**: The Dataflow pipeline that streams changes from PubSub and
  into BigQuery does **not** handle changes of schema. If you want to update the
  schema of one of your MySQL tables, it is a good idea to redeploy the Debezium
  connector, and the Dataflow pipeline.
