#!/bin/bash

python ../speed_layer/live_data_producer.py > ../logs/live_data_producer_logs.log 2>&1 &
echo "Launched Live Data Producer"

sleep 5

python ../speed_layer/listener_spark_streaming.py > ../logs/live_data_listener_logs.log 2>&1 &
echo "Launched Live Data Listener"

sleep 2

python ../flask/flask_front.py > ../logs/flask_logs.log 2>&1 &
echo "Launched Live Data Producer"

./sub_proc_historical_data.sh > ../logs/sub_proc_historical_data.log 2>&1 &
echo "Launched Historical Data Pipeline"