# Neo4j Flex Templates

This project contains FlexTemplates that facilitate loading files within the Google Cloud to the Neo4j graph database

## Version History

v. 0.10 initial PR

## Introductory Blog

Neo4j has released flex templates for GCP Dataflow which support complex ETL processes through configuration not code.  This capability fills a gap for joint GCP and Neo4j customers who are looking for cloud native data integration without having to manage Spark services.  Over the past decade, graph databases have become an invaluable tool for discovering fraud, understanding network operations and supply chains, disambiguating identities, and providing recommendations – among other things.  Now, BigQuery, Spanner, and Google Cloud Storage customers will be able to easily leverage graphs to mine insights in the data.

There are many ways to move data into Neo4j.  The most popular approach for bulk loading Neo4j is the LOAD CSV cypher command from any client connection such as Java, Python, Go, .NET, Node, Spring and others.  Data scientists tend to favor the Neo4j Spark connector and Data Warehouse connector, which both run on DataProc and are easily incorporated into python notebooks.  For individual users, the graphical ETL import tool is very convenient and for enterprises needing lifecycle management, Apache Hop, a project co-sponsored by Neo4j, is a great option.

The Dataflow approach is interesting and different for a few reasons.  Although it requires a customized JSON configuration file, that’s all that is required.  No notebooks, no Spark environment, no code, no cost when the system is idle.  Also, Dataflow runs within the context of GCP security so if a resource is accessible to the project and service account there is no need to track and secure another resource locator and set of credentials.  Finally, the Neo4j flex  template implements Neo4j java API best practices.

These features make this solution ideal for copy-and-paste re-use between customer environments.  For example, a best-practices mapping that loads Google Analytics (GA) from BigQuery to Neo4j could be leveraged by any GA customer.   ISVs may leverage this capability to move their solutions to the Google cloud and Google Data Lake adopters will accelerate their adoption of graph as an essential side-car service in their reference data architectures.

## Executing Template Example

The template requires the following parameters:

* jobSpecUri: GS hosted job specification file
* neo4jConnectionUri: GS hosted Neo4j configuration file
* inputFilePattern: (Optional) Job spec source override with GS text file
* readQuery: (Optional) Job spec source override with query
* optionsJson: (Opitonal) JSON formatted string to supply runtime variables that replace $ delimited variables

Template can be executed using the following gcloud command:

```sh
export TEMPLATE_GCS_LOCATION="gs://neo4j-dataflow/flex-templates/images/gcp-to-neo4j-image-spec.json"
export REGION=us-central1
export MACHINE_TYPE=n2-highmem-8
 
gcloud dataflow flex-template run "test-text-cli-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location="$TEMPLATE_GCS_LOCATION" \
    --region "$REGION" \
    --parameters jobSpecUri="gs://neo4j-dataflow/job-specs/testing/text/text-northwind-jobspec.json" \
    --parameters neo4jConnectionUri="gs://neo4j-dataflow/job-specs/testing/common/auradb-free-connection.json" \
    --max-workers=1 \
    --worker-machine-type=${MACHINE_TYPE} 
```

## Building Project

#### Compiling the pipeline

Execute the following command from the directory containing the root pom.xml:

```sh
export JAVA_HOME=`/usr/libexec/java_home -v 8`
mvn -DskipTests=true clean compile -pl v2/googlecloud-to-neo4j -am -f unified-templates.xml
```

#### Execuing unit tests

Execute the following command from the directory containing the root pom.xml:

```shell script
mvn clean test -pl v2/googlecloud-to-neo4j -am -f unified-templates.xml
```

### Building Container Image

* Set environment variables that will be used in the build process.
* Note that /template is the working directory inside the container image

```sh
export PROJECT=neo4jbusinessdev
export GS_WORKING_DIR=gs://neo4j-sandbox/dataflow-working
export APP_NAME=gcp-to-neo4j
export REGION=us-central1
export MACHINE_TYPE=n2-highmem-8
export IMAGE_NAME=gcp-to-neo4j
export BUCKET_NAME=gs://neo4j-dataflow/flex-templates
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_POM_MODULE=googlecloud-to-neo4j
export APP_ROOT=/template/${APP_NAME}
export COMMAND_SPEC=${APP_ROOT}/resources/${APP_NAME}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${APP_NAME}-image-spec.json
gcloud config set project ${PROJECT}
```

* Build and push image to Google Container Repository
* Execute the following command from the directory containing the root pom.xml:

```sh
mvn -DskipTests=true clean package \
    -f unified-templates.xml \
    -pl v2/${TEMPLATE_POM_MODULE} \
    -am \
    -Djib.container.mainClass=com.google.cloud.teleport.v2.neo4j.GcpToNeo4j \
    -Dimage=${TARGET_GCR_IMAGE} \
    -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
    -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
    -Dapp-root=${APP_ROOT} \
    -Dcommand-spec=${COMMAND_SPEC} 
```

### Creating Image Spec

Create file in Cloud Storage with path to container image in Google Container Repository.

```sh
echo "{
  \"image\": \"${TARGET_GCR_IMAGE}\",
  \"metadata\": {
    \"name\": \"Google Cloud to Neo4j\",
    \"description\": \"BigQuery, Text, and other source import into Neo4j\",
    \"parameters\": [
      {
        \"name\": \"jobSpecUri\",
        \"label\": \"Job configuration file\",
        \"helpText\": \"Configuration, source and target metadatga\",
        \"paramType\": \"TEXT\",
        \"isOptional\": false
      }, 
      {
        \"name\": \"neo4jConnectionUri\",
        \"label\": \"Neo4j connection metadata\",
        \"helpText\": \"Neo4j connection metadata json file\",
        \"paramType\": \"TEXT\",
        \"isOptional\": false
      },  
      {
        \"name\": \"inputFilePattern\",
        \"label\": \"Text file\",
        \"helpText\": \"Override text file pattern (optional)\",
        \"paramType\": \"TEXT\",
        \"isOptional\": true
      } ,  
      {
        \"name\": \"readQuery\",
        \"label\": \"Query SQL\",
        \"helpText\": \"Override SQL query (optional)\",
        \"paramType\": \"TEXT\",
        \"isOptional\": true
      } ,  
      {
        \"name\": \"optionsJson\",
        \"label\": \"Options JSON\",
        \"helpText\": \"Runtime tokens like: {token1:value1,token2:value2}\",
        \"paramType\": \"TEXT\",
        \"isOptional\": true
      }
      ]
    },
    \"sdk_info\": {
       \"language\": \"JAVA\"
    }
  }" > image_spec.json
gsutil cp image_spec.json ${TEMPLATE_IMAGE_SPEC}
cp image_spec.json ./v2/googlecloud-to-neo4j/docs/gcp-to-neo4j-image-spec.json
rm image_spec.json
```

## Other resources

    https://cloud.google.com/sdk/gcloud/reference/dataflow/flex-template/build
    https://cloud.google.com/sdk/gcloud/reference/dataflow/flex-template/run

## Known issues

### Known limitations

- This is not implemented in the Text writer since order by operations do not work well in Beam SQL
- For any one label, insertion parallelism will be limited by parameter, but these will accumulate over many nodes. This
  is by design currently.

### Roadmap

- Support for reading data from other non-SQL sources including Avro, Parquet, and MongoDb
- Support for reading data from other SQL based sources including Spanner and Postgres
- Support for auditing writes to Parquet on GCS
- Supporting join transformations inside the job
- Support for write back to Neo4j
- Implement automap to auto-generate properties
- Performance benchmark documentation

## Running Apache Hop

export JAVA_HOME=`/usr/libexec/java_home -v 8`
cd ~/Documents/hop
./hop-gui.sh

### Testing Template

The template unit tests can be run using:

```sh
mvn test
```


## Maintainer

    Anthony Krinsky 
    Sr. Partner Solution Architect
    anthony.krinsky@neo4j.com

Note that test scripts point to my auraDb instance.  AuraDb is free up to 50,000 nodes/edges.  
Great for testing but don't forget to manually "resume" it if inactive for 3 days.
    
    https://console.neo4j.io

