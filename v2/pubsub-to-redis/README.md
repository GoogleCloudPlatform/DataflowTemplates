# PubSub to Redis Dataflow Template

The [PubSubToRedis](src/main/java/com/google/cloud/teleport/v2/templates/PubSubToRedis.java) pipeline
ingests data from a PubSub subscription and writes the data to Redis.

## Getting Started

### Requirements
* Java 11
* Maven
* PubSub Subscription exists
* Redis DB exists and is operational

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.
Set the pipeline vars
```sh
export PROJECT=<my-project>
export IMAGE_NAME=<my-image-name>
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=pubsub-to-redis
export APP_ROOT=/template/${TEMPLATE_MODULE}
export COMMAND_SPEC=${APP_ROOT}/resources/${TEMPLATE_MODULE}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${TEMPLATE_MODULE}-image-spec.json

export SUBSCRIPTION=<my-subscription>
export REDIS_HOST=<my-port>
export REDIS_PORT=<my-port>
```

* Build and push image to Google Container Repository
```sh
mvn clean package -Dimage=${TARGET_GCR_IMAGE} \
                  -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
                  -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
                  -Dapp-root=${APP_ROOT} \
                  -Dcommand-spec=${COMMAND_SPEC} \
                  -am -pl ${TEMPLATE_MODULE}
```

### Building and Testing Template locally

###### Clone
```sh
git clone https://github.com/redis-field-engineering/DataflowTemplates.git && cd DataflowTemplates
```

###### Build
```sh
mvn clean package -pl v2/pubsub-to-redis -am -Dmaven.test.skip -Djib.skip
```

###### Test
```sh
mvn test -pl v2/pubsub-to-redis -am
```

### Executing Template

The template requires the following parameters:
* redisHost: Redis database host , ex: 127.0.0.1
* redisPort: Redis database port , ex: 6379
* redisPassword: Redis database password , ex: redis123
* inputSubscription: PubSub subscription to read from, ex: projects/my-project/subscriptions/my-subscription

The template has the following optional parameters:
* redisSinkType: STRING_SINK, HASH_SINK and STREAMS_SINK (defaults to STRING_SINK)
* sslEnabled: true or false (defaults to false)
* timeout: Redis connection timeout (defaults to 2000 ms)
* ttl: Redis hash keys time to live in sec (defaults to -1 i.e. no expiration)

Template can be executed using the following gcloud command.
````sh
export GOOGLE_APPLICATION_CREDENTIALS=<SERVICE_ACCOUNT_JSON_FILE>
````
```sh
gcloud dataflow flex-template run pubsub-to-redis-$(date +'%Y%m%d%H%M%S') \
--template-file-gcs-location gs://redis-field-engineering/redis-field-engineering/pubsub-to-redis/flex/Cloud_PubSub_to_Redis \
--project central-beach-194106 \
--region us-central1 \
--parameters inputSubscription=projects/central-beach-194106/subscriptions/pubsub-to-redis, \
--parameters redisHost=<REDIS_DB_HOST>, \
--parameters redisPort=<REDIS_DB_PORT>, \
--parameters redisPassword=<REDIS_DB_PASSWORD> \
--parameters redisSinkType=<STRING_SINK|HASH_SINK|STREAMS_SINK> (defaults to STRING_SINK)
```
