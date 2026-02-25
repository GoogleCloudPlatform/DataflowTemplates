# Release Process

Templates are released in a weekly basis (best-effort) as part of the efforts to
keep [Google-provided Templates](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates) updated with latest fixes and improvements.

In case desired, you can stage and use your own changes using the `Staging (Deploying) Templates` steps.

To execute the release of multiple templates, we provide a single Maven command to release Templates, which is a shortcut to
stage all templates while running additional validations.

```shell
mvn clean verify -PtemplatesRelease \
  -DprojectId="{projectId}" \
  -DbucketName="{bucketName}" \
  -DlibrariesBucketName="{bucketName}-libraries" \
  -DstagePrefix="$(date +%Y_%m_%d)-00_RC00"
```

To execute the release of a subset of flex templates, the following command can be used. For example,

```shell
mvn clean verify -PtemplatesRelease \
  -DprojectId="{projectId}" \
  -DbucketName="{bucketName}" \
  -DlibrariesBucketName="{bucketName}-libraries" \
  -DstagePrefix="$(date +%Y_%m_%d)-00_RC00" \
  -pl v2/streaming-data-generator -am
```
