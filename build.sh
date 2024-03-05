set -x
. env.sh
time mvn compile -pl v2/googlecloud-to-googlecloud exec:java -Dexec.mainClass=com.google.cloud.teleport.v2.templates.BigtableChangeStreamsToLogs
