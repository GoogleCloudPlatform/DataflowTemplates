#!/bin/bash
set -e

for i in {1..4}; do
    echo "======================================"
    echo "Starting test run $i at $(date)..."
    echo "======================================"
    
    echo "Submitting load generator..."
    mvn compile exec:java \
        -Dexec.mainClass=com.google.cloud.teleport.v2.templates.SpannerConcurrencyLoadGenerator \
        -Dexec.args="--runner=DataflowRunner --project=span-cloud-migrations-testing --projectId=span-cloud-migrations-testing --region=us-central1 --instanceId=pratick-load-test --databaseId=poc-source --numWorkers=100 --maxNumWorkers=100" \
        -Dexec.cleanupDaemonThreads=false \
        -pl v2/spanner-to-sourcedb
    
    echo "Load generator submitted. Waiting exactly 5 minutes for it to generate load..."
    sleep 300
    
    echo "Canceling load generator..."
    JOB_ID=$(gcloud dataflow jobs list --project=span-cloud-migrations-testing --status=active --format="value(JOB_ID)" --filter="name:spannerconcurrencyloadgenerator*")
    if [ ! -z "$JOB_ID" ]; then
        gcloud dataflow jobs cancel $JOB_ID --project=span-cloud-migrations-testing
        echo "Cancelled job $JOB_ID. Waiting for dataflow to catch up..."
    else
        echo "No active load generator job found to cancel."
    fi
    
    # Wait a bit before checking validation
    sleep 60
    
    while true; do
        echo "Running DataValidator at $(date)..."
        # Run DataValidator and capture output
        VALIDATOR_OUT=$(mvn test-compile exec:java -Dexec.mainClass="com.google.cloud.teleport.v2.templates.DataValidator" -Dexec.classpathScope=test -Dexec.cleanupDaemonThreads=false -Dcheckstyle.skip=true -pl v2/spanner-to-sourcedb | tee /dev/tty)
        
        # Check if mismatches is 0
        if echo "$VALIDATOR_OUT" | grep -q "Mismatches: 0"; then
            echo "Data validation succeeded with 0 mismatches!"
            break
        else
            echo "Data validation found mismatches (Dataflow still processing). Sleeping for 5 minutes..."
            sleep 300
        fi
    done
    
    echo "Test run $i completed successfully at $(date)!"
    echo ""
done

echo "All 4 test runs completed successfully!"
