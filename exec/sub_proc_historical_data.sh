#!/bin/bash

while true; do
    # Get the current minute
    MINUTE=$(date +"%M")

    # If the current time is the start of the hour (minute 0)
    if [ "$MINUTE" -eq "00" ]; then
        # Run your script or command here
        cd ../batch_layer/
        python ../batch_layer/data_ingestion.py > ../logs/historical_data_producer_logs.log 2>&1
        python ../batch_layer/hdfs_to_redis.py > ../logs/hdfs_to_redis_logs.log 2>&1
        cd ../exec
    fi

    # Sleep for 60 seconds before checking again
    sleep 60
done