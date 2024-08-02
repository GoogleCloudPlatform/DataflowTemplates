#!/bin/bash

db_tfvars="gs://tf-demo-resources/db.tfvars"
vpc_tfvars="gs://tf-demo-resources/vpc.tfvars"
migrate_tfvars="gs://tf-demo-resources/migrate.tfvars"
session_file="gs://tf-demo-resources/session.json"

db_dir="v2/spanner-common/terraform/samples/mysql-vpc-spanner-test-infra/"
vpc_dir="v2/spanner-common/terraform/samples/configure-shared-vpc"
migrate_dir="v2/datastream-to-spanner/terraform/samples/mysql-sharded-end-to-end/"

if [ -z "$USER" ]; then
  echo "Error: USER environment variable is not set."
  exit 1
fi

gcloud storage cp "$db_tfvars" .
gcloud storage cp "$vpc_tfvars" .
gcloud storage cp "$migrate_tfvars" .
gcloud storage cp "$session_file" .

echo "Modifying template tfvars with username:" $USER

sed -i '' "s/__USER__/$USER/g" db.tfvars
sed -i '' "s/__USER__/$USER/g" vpc.tfvars
sed -i '' "s/__USER__/$USER/g" migrate.tfvars

echo "Replacement complete. You can now run Terraform."

mv db.tfvars "$db_dir"
mv vpc.tfvars "$vpc_dir"
mv migrate.tfvars "$migrate_dir"
mv session.json "$migrate_dir"