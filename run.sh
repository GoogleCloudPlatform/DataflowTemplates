set -x
# mvn -e package -PtemplatesRun \
#   -DskipTests \
#   -Dcheckstyle.skip \
#   -DprojectId="$PROJECT" \
#   -DbucketName="$BUCKET" \
#   -DbigtableReadInstanceId=meagar-test \
#   -DbigtableChangeStreamAppProfile=default \
#   -DbigtableReadTableId=meagar-test \
#   -Dregion="us-central1" \
#   -DtemplateName="Bigtable_Change_Streams_to_Logs" \
#   -Dparameters="bigtableChangeStreamAppProfile=default,bigtableReadTableId=meagar-test,bigtableReadInstanceId=meagar-test" \
#   -pl v2/googlecloud-to-googlecloud -am \
#   -DjobName="meagar-test-job2"


#-Dexec.args="--runner=direct --arg1=val1 --arg2=val2 ..." \

. env.sh

ARGS="\
--runner=direct \
--windowDuration=2s \
--bigtableChangeStreamAppProfile=default \
--bigtableReadInstanceId=meagar-test \
--bigtableReadTableId=meagar-test \
--project=$PROJECT \
--embeddingColumn=cf1:embeddings \
--crowdingTagColumn=cf1:crowding_tag \
--allowRestrictsMappings=cf1:allow->allow1,cf1:allow2->allow2 \
--denyRestrictsMappings=cf1:deny->deny\
--intNumericRestrictsMappings=cf1:int->intrestrict \
--floatNumericRestrictsMappings=cf1:float->floatrestrict \
--doubleNumericRestrictsMappings=cf1:double->doublerestrict1,cf1:double2->doublerestrict2 \
"

mvn compile exec:java \
  -Dexec.mainClass=com.google.cloud.teleport.v2.templates.bigtablechangestreamstovectorsearch.BigtableChangeStreamsToVectorSearch \
  -Dexec.args="$ARGS" \
  -pl googlecloud-to-googlecloud \
  -f v2/pom.xml
