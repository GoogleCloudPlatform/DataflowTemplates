# mvn -e package -PtemplatesStage  \
#   -DskipTests \
#   -Dcheckstyle.skip \
#   -DprojectId="$PROJECT" \
#   -DbucketName="$BUCKET" \
#   -DstagePrefix="images/$(date +%Y_%m_%d)_01" \
#   -DtemplateName="Bigtable_Change_Streams_to_Logs" \
#   -Dparameters="fooBar=1" \
#   -Dexec.args="--project=$PROJECT --output=$BUCKET" \
#   -pl v2/googlecloud-to-googlecloud -am

set -x
. ./env.sh
time mvn package -e -PtemplatesStage \
    -DskipTests \
    -Dcheckstyle.skip \
    -DprojectId="$PROJECT" \
    -DbucketName="$BUCKET" \
    -DstagePrefix="images/$(date +%Y_%m_%d)_01" \
    -DtemplateName="Bigtable_Change_Streams_to_Logs" \
    -pl v2/googlecloud-to-googlecloud -am \
    -Dparameters="--bigtableChangeStreamAppProfile=default bigTableChangeStreamAppProfile=default" \
    -DbigtableChangeStreamAppProfile=default
