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

#### Creating Image Spec

* Create spec file in Cloud Storage under the path ${TEMPLATE_IMAGE_SPEC} describing container image location and metadata.
```json
{
  "name": "PubSub To Redis",
  "description": "A pipeline reads from pubsub and writes to redis.",
  "parameters": [
    {
      "name": "inputSubscription",
      "label": "PubSub Subscription name",
      "helpText": "Name of pubsub Subscription. ex: projects/<project-id>/subscriptions/<subscription-name>",
      "is_optional": false,
      "regexes": [
        "^projects\\/[^\\n\\r\\/]+\\/subscriptions\\/[^\\n\\r\\/]+$"
      ],
      "paramType": "PUBSUB_SUBSCRIPTION"
    },
    {
      "name": "redisHost",
      "label": "Redis database host",
      "helpText": "Redis database endpoint host",
      "is_optional": false,
      "regexes": [
        "[a-zA-Z0-9._\\-]+"
      ],
      "paramType": "TEXT"
    },
    {
      "name": "redisPort",
      "label": "Redis database port",
      "helpText": "Redis database endpoint port",
      "is_optional": false,
      "paramType": "TEXT"
    },
    {
      "name": "redisPassword",
      "label": "Redis database password",
      "helpText": "Redis database endpoint password",
      "is_optional": false,
      "paramType": "TEXT"
    }
  ]
}
```

### Testing Template

The template unit tests can be run using:
```sh
mvn test
```

### Executing Template

The template requires the following parameters:
* redisHost: Redis database host , ex: 127.0.0.1
* redisPort: Redis database port , ex: 6379
* redisPassword: Redis database password , ex: redis123
* inputSubscription: PubSub subscription to read from, ex: projects/my-project/subscriptions/my-subscription


Template can be executed using the following gcloud command.
```sh
gcloud dataflow flex-template run pubsub-to-redis-$(date +'%Y%m%d%H%M%S') \
--template-file-gcs-location gs://redis-field-engineering/redis-field-engineering/pubsub-to-redis/flex/Cloud_PubSub_to_Redis \
--project central-beach-194106 \
--region us-central1 \
--parameters inputSubscription=projects/central-beach-194106/subscriptions/pubsub-to-redis, \
--parameters redisHost=<REDIS_DB_HOST>, \
--parameters redisPort=<REDIS_DB_PORT>, \
--parameters redisPassword=<REDIS_DB_PASSWORD>
```
