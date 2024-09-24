
pkill -f ../speed_layer/live_data_producer.py
echo "Killed Live Data Producer"

pkill -f ../speed_layer/listener_spark_streaming.py
echo "Killed Live Data Listener"

pkill -f ../flask/flask_front.py
echo "Killed Flask webpage"

pkill -f script_name.sh
echo "Killed Historical Data Pipeline"