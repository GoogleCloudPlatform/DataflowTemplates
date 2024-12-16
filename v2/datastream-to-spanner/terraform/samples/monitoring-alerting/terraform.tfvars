# Project-specific Variables
project_id = "<YOUR_PROJECT_ID>"  # The Google Cloud Project ID used for the source project and deployment of all components related to the Spanner Migration Tool.
spanner_project_id = "<YOUR_SPANNER_PROJECT_ID>"  # The Google Cloud Project ID where the Spanner dashboard will be deployed.
alerting_project_id = "<YOUR_ALERTING_PROJECT_ID>"  # Specify the Project ID for alerting module deployment.


# Specify a prefix for job IDs to monitor multiple jobs with the same prefix across Datastream, Dataflow, and Cloud Storage.
prefix = "<PREFIX_FOR_JOBS>"
