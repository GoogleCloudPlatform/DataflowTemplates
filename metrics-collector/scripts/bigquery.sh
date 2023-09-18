# Substitute below variables
project_id=test-project
dataset_name=test-dataset
table_name=test-table


# Create dataset
bq --project_id ${project_id} mk ${dataset_name}

#Delete table (if exists)
bq --project_id ${project_id} rm -f -t ${dataset_name}.${table_name}

# Create table (from project root)
bq --project_id ${project_id} mk -t --description "Contains job run statistics" ${dataset_name}.${table_name} \
./metrics_bigquery_schema.json

# List tables in dataset
bq --project_id ${project_id} ls ${dataset_name}

# View table schema
bq --project_id ${project_id} show ${dataset_name}.${table_name}