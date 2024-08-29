#!/bin/bash
# This is a sample script to help you generate the shard configuration json for a sharded bulk migration.
# For the sake of simplicity, the script assumes that:
# 1. All shards share the same access credentials.
# 2. All the physical shards share the same names for logical databases.
#
# Please modify the script as required for your use case.
# Dependency: `bash` and `jq`

# Steps to run this script
# Step-1 Update the variables to match your use case.
# Step-2 Run the script.

# Variables

# Replace the below with the path in GCS where you would like to upload the file.
OUTPUT_PATH="gs://bucket/path/to/file.json"

# Replace and add the Ips of all physical shards you would like to migrate in the array below.
host_ips=(
"ip.of.first.shard"
"ip.of.second.shard"
)
# Network port of your database, generally 3306 for Mysql.
port="3306"
# Replace and add the names of all the logical databases you would like to migrate.
# Note that all logical databases must have the same schema.
database_names=("logicalDb01" "logicalDb02")
# Replace with the actual DB credentials.
username="<user-name>"
password="<password>"

# The script below helps you create the shard config json.
# Main JSON Structure.
json='{
  "configType": "dataflow",
  "shardConfigurationBulk": {
    "schemaSource": {
      "dataShardId": "",
      "host": "",
      "user": "",
      "password": "",
      "port": "",
      "dbName": ""
    },
    "dataShards": []
  }
}'

# Code below populates the dataShards array.
data_shards=()
for host_ip in "${host_ips[@]}"; do
    data_shard_id=$(echo "$host_ip" | tr '.' '-')
    databases=()
    for database_name in "${database_names[@]}"; do
        database_id="$data_shard_id-$database_name"
        database='{
          "dbName": "'"$database_name"'",
          "databaseId": "'"$database_id"'",
          "refDataShardId": "'"$data_shard_id"'"
        }'
        databases+=("$database")
    done
    databases_json=$(printf "%s" "${databases[@]}" | jq -s 'map(.) | [.[]]')

    data_shard='{
      "dataShardId": "'"$data_shard_id"'",
      "host": "'"$host_ip"'",
      "user": "'"$username"'",
      "password": "'"$password"'",
      "port": "'"$port"'",
      "dbName": "",
      "databases": '"${databases_json}"'
    }'
    data_shards+=("$data_shard")
done

datashards_json=$(printf "%s" "${data_shards[@]}" | jq -s 'map(.) | [.[]]')

# Updates the main JSON with dataShards.
json=$(echo "$json" | jq '.shardConfigurationBulk.dataShards = '"${datashards_json}"'')

# Outputs the final JSON.
generate_json() {
  echo "$json"
}

ts() {
  date '+%Y-%m-%d-%H-%M-%S-%N'
}


# Generates the JSON and tee it into a temporary file.
temp_file=$(mktemp "shard-config-$(ts)-XXXX.json")
generate_json | tee "$temp_file"

# Uploads the temporary file to GCS.
gsutil cp "$temp_file" "${OUTPUT_PATH}"

# Cleans up the temporary file.
rm "$temp_file"