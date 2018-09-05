mvn compile exec:java \
  -Dexec.mainClass=com.google.cloud.teleport.templates.TextIOToBigQuery \
  -Dexec.cleanupDaemonThreads=false \
  -Dexec.args=" \
    --project=creative-analytics \
    --stagingLocation=gs://adevents.playground.xyz/staging \
    --tempLocation=gs://adevents.playground.xyz/temp \
    --templateLocation=gs://adevents.playground.xyz/templates/TextIOToBigQuery.json \
    --runner=DataflowRunner"
