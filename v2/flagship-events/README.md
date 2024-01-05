# Flagship Events Dataflow Job

## Build and Deploy

### Integration

```sh
  gcloud config set project is-events-dataflow-intg

  mvn clean package -pl v2/flagship-events -am

  gcloud dataflow flex-template build gs://is-events-dataflow-intg/templates/flagship-events.json \
    --image-gcr-path "us-west1-docker.pkg.dev/is-events-dataflow-intg/default/dataflow/flagship-events:latest" \
    --sdk-language "JAVA" \
    --flex-template-base-image JAVA11 \
    --metadata-file "v2/flagship-events/metadata.json" \
    --jar "v2/flagship-events/target/flagship-events-1.0-SNAPSHOT.jar" \
    --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.keap.dataflow.flagshipevents.FlagshipEventsPubsubToBigQuery"
     
  mvn clean package -PtemplatesStage  \
    -DskipTests \
    -DprojectId="is-events-dataflow-intg" \
    -DbucketName="templates" \
    -DstagePrefix="templates" \
    -DtemplateName="flagship-events" \
    -pl v2/flagship-events \
    -am

  gcloud dataflow flex-template run "flagshipevents-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "gs://is-events-dataflow-intg/templates/flagship-events.json" \
    --parameters env="intg"
```