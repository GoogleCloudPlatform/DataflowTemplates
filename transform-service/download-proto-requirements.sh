# TODO(pabloem): Parameterize this script with the Beam Version used to build the Syndeo template

mkdir -p src/main/proto/org/apache/beam/model/pipeline/v1/
cd src/main/proto/org/apache/beam/model/pipeline/v1/

curl https://raw.githubusercontent.com/apache/beam/v2.42.0/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/beam_runner_api.proto -o beam_runner_api.proto
curl https://raw.githubusercontent.com/apache/beam/v2.42.0/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/schema.proto -o schema.proto
curl https://raw.githubusercontent.com/apache/beam/v2.42.0/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/endpoints.proto -o endpoints.proto
