# Flagship Events Dataflow Job

## Build and Deploy

### Build the Uber Jar

In order to have the Conscrypt jar packaged you will need to build the Uber Jar first
```sh
mvn clean package
```

### Deploy

Then you can deploy to a given environment by calling the deploy script with one of "intg", "stge" or "prod":
```sh
./bin/deploy-flagship-events.sh --env {env}
```

### Validate

You can watch records being added to the BigQuery table in each environment as API calls are made:

* [Intg](https://console.cloud.google.com/bigquery?walkthrough_id=dataflow_index&project=is-events-dataflow-intg&ws=!1m5!1m4!4m3!1sis-events-dataflow-prod!2scrm_prod!3sapi_call_made)
* [Stge](https://console.cloud.google.com/bigquery?walkthrough_id=dataflow_index&project=is-events-dataflow-stge&ws=!1m5!1m4!4m3!1sis-events-dataflow-prod!2scrm_prod!3sapi_call_made)
* [Prod](https://console.cloud.google.com/bigquery?walkthrough_id=dataflow_index&project=is-events-dataflow-prod&ws=!1m5!1m4!4m3!1sis-events-dataflow-prod!2scrm_prod!3sapi_call_made)

Once validated in Intg or Stge shut down the job so as to not consume extra resources:

* [Intg](https://console.cloud.google.com/dataflow/jobs?project=is-events-dataflow-intg&walkthrough_id=dataflow_index)
* [Stge](https://console.cloud.google.com/dataflow/jobs?project=is-events-dataflow-stge&walkthrough_id=dataflow_index)