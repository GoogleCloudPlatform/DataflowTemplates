gcloud dataflow flex-template build gs://dataflow-work-bucket/images/image-spec.json \
 --image-gcr-path "us-central1-docker.pkg.dev/data-platform-441421/flex-templates-stg/datastream-to-bigquery-jdk11:latest" \
 --sdk-language "JAVA" \
 --flex-template-base-image JAVA11 \
 --metadata-file "datastream-to-bigquery/image_spec.json" \
 --jar "datastream-to-bigquery/target/datastream-to-bigquery-1.0-SNAPSHOT.jar" \
 --jar "datastream-to-bigquery/target/extra_libs/conscrypt-openjdk-uber.jar" \
 --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.google.cloud.teleport.v2.templates.DataStreamToBigQuery" 