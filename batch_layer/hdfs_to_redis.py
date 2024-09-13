from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType
from redistimeseries.client import Client
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import pandas as pd
import subprocess
import re



spark = SparkSession.builder \
.appName("Crypto Data Extraction") \
.config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
.getOrCreate()

hdfs_path = "hdfs://localhost:9000/crypto_data/"

with open('crypto_details.csv', 'r') as file:
    line_count = sum(1 for line in file)

# List files with details
cmd = f"hdfs dfs -ls {hdfs_path}"
proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
output = proc.communicate()[0].decode('utf-8')

# Filter and sort by date
files = []
for line in output.splitlines():
    parts = line.split()
    if len(parts) == 8:
        date_time = f"{parts[5]} {parts[6]}"
        file_path = parts[7]
        files.append((date_time, file_path))

# Sort files by date and time (most recent first)
files.sort(reverse=True, key=lambda x: x[0])

# Return the most recent n files
file_list = [file[1] for file in files[:(line_count-1)*2]]
#print(file_list)

for file in file_list:

    match = re.search(r'days_(.*?)_2024', file)
    if match:
        acronym = match.group(1)  # Extract the matched part
        print(f"Found substring: {acronym}")
    else:
        print("No match found")

    match = re.search(r'data/(.*?)_', file)
    if match:
        type_data = match.group(1)  # Extract the matched part
        print(f"Found substring: {type_data}")
    else:
        print("No match found")
    
    df = spark.read.format("parquet").load(file)

    df = df.withColumn("timestamp", F.floor(F.col("timestamp")/1000 ))

    rts = Client(host='127.0.0.1', port=6379)

    match type_data:

        case "price":
            
            window_spec_4d = Window.orderBy(F.col("timestamp")).rowsBetween(-96, 0)
            window_spec_week = Window.orderBy(F.col("timestamp")).rowsBetween(-168, 0)

            # Calculate the moving average
            df = df.withColumn("moving_avg_4d", F.avg(F.col("price")).over(window_spec_4d).cast(FloatType()))
            df = df.withColumn("moving_avg_week", F.avg(F.col("price")).over(window_spec_week).cast(FloatType()))

            # Calculate the standard deviation
            df = df.withColumn("std_dev_4d", F.stddev(F.col("price")).over(window_spec_4d).cast(FloatType()))
            df = df.withColumn("std_dev_week", F.stddev(F.col("price")).over(window_spec_week).cast(FloatType()))

            df = df.withColumn("moving_avg_4d", F.when(F.col("moving_avg_4d") == None, "price").otherwise(F.col("moving_avg_4d")))
            df = df.withColumn("moving_avg_week", F.when(F.col("moving_avg_week") == None, "price").otherwise(F.col("moving_avg_week")))
            df = df.withColumn("std_dev_4d", F.when(F.col("std_dev_4d").isNull(), 0).otherwise(F.col("std_dev_4d")))
            df = df.withColumn("std_dev_week", F.when(F.col("std_dev_week").isNull(), 0).otherwise(F.col("std_dev_week")))

            # Calculate the Bollinger Bands
            df = df.withColumn("up_bollinger_band_4d", F.col("moving_avg_4d") + 2*F.col("std_dev_4d"))
            df = df.withColumn("low_bollinger_band_4d", F.col("moving_avg_4d") - 2*F.col("std_dev_4d"))

            df = df.withColumn("up_bollinger_band_week", F.col("moving_avg_week") + 2*F.col("std_dev_week"))
            df = df.withColumn("low_bollinger_band_week", F.col("moving_avg_week") - 2*F.col("std_dev_week"))

            df_price = df.select("timestamp" , "price")
            df_moving_avg_4d = df.select("timestamp" , "moving_avg_4d")
            df_moving_avg_week = df.select("timestamp" , "moving_avg_week")
            df_up_bollinger_band_4d = df.select("timestamp" , "up_bollinger_band_4d")
            df_low_bollinger_band_4d = df.select("timestamp" , "low_bollinger_band_4d")
            df_up_bollinger_band_week = df.select("timestamp" , "up_bollinger_band_week")
            df_low_bollinger_band_week = df.select("timestamp" , "low_bollinger_band_week")

            list_price = [("90_DAYS_PRICE:"+acronym,) + tuple(row) for row in df_price.collect()]
            list_moving_avg_4d = [("90_DAYS_MOVING_AVERAGE_4D:"+acronym,) + tuple(row) for row in df_moving_avg_4d.collect()]
            list_moving_avg_week = [("90_DAYS_MOVING_AVERAGE_WEEK:"+acronym,) + tuple(row) for row in df_moving_avg_week.collect()]
            list_up_bollinger_band_4d = [("90_DAYS_UP_BOLLINGER_BAND_4D:"+acronym,) + tuple(row) for row in df_up_bollinger_band_4d.collect()]
            list_up_bollinger_band_week = [("90_DAYS_UP_BOLLINGER_BAND_WEEK:"+acronym,) + tuple(row) for row in df_up_bollinger_band_week.collect()]
            list_low_bollinger_band_4d = [("90_DAYS_LOW_BOLLINGER_BAND_4D:"+acronym,) + tuple(row) for row in df_low_bollinger_band_4d.collect()]
            list_low_bollinger_band_week = [("90_DAYS_LOW_BOLLINGER_BAND_WEEK:"+acronym,) + tuple(row) for row in df_low_bollinger_band_week.collect()]


            rts.madd(list_price)
            rts.madd(list_moving_avg_4d)
            #rts.madd(list_moving_avg_week)
            rts.madd(list_up_bollinger_band_4d)
            #rts.madd(list_up_bollinger_band_week)
            rts.madd(list_low_bollinger_band_4d)
            #rts.madd(list_low_bollinger_band_week)

        case "volume":
            df_volume = df.select("timestamp" , "volume")
            list_volume = [("90_DAYS_VOLUME:"+acronym,) + tuple(row) for row in df_volume.collect()]
            rts.madd(list_volume)

'''
df_price = df.select("NOM_IMPORT", "timestamp" , "price")
df_moving_avg_4d = df.select("NOM_IMPORT", "timestamp" , "moving_avg_4d")
df_moving_avg_week = df.select("NOM_IMPORT", "timestamp" , "moving_avg_week")
df_up_bollinger_band_4d = df.select("NOM_IMPORT", "timestamp" , "up_bollinger_band_4d")
df_low_bollinger_band_4d = df.select("NOM_IMPORT", "timestamp" , "low_bollinger_band_4d")
df_up_bollinger_band_week = df.select("NOM_IMPORT", "timestamp" , "up_bollinger_band_week")
df_low_bollinger_band_week = df.select("NOM_IMPORT", "timestamp" , "low_bollinger_band_week")

rts = Client(host='127.0.0.1', port=6379)

list_price = [tuple(row) for row in df_price.collect()]
list_volume = [tuple(row) for row in df_volume.collect()]

rts.madd(list_price)
rts.madd(list_volume)
'''

'''
df = spark.read.format("parquet").load("hdfs://localhost:9000/crypto_data/price_90_days_avalanche-2_2024-09-12_00-31.parquet")

window_spec_4d = Window.orderBy(F.col("timestamp")).rowsBetween(-96, 0)
window_spec_week = Window.orderBy(F.col("timestamp")).rowsBetween(-168, 0)

# Calculate the moving average
df = df.withColumn("moving_avg_4d", F.avg(F.col("price")).over(window_spec_4d).cast(FloatType()))
df = df.withColumn("moving_avg_week", F.avg(F.col("price")).over(window_spec_week).cast(FloatType()))

# Calculate the standard deviation
df = df.withColumn("std_dev_4d", F.stddev(F.col("price")).over(window_spec_4d).cast(FloatType()))
df = df.withColumn("std_dev_week", F.stddev(F.col("price")).over(window_spec_week).cast(FloatType()))

# Calculate the Bollinger Bands
df = df.withColumn("up_bollinger_band_4d", F.col("moving_avg_4d") + 2*F.col("std_dev_4d"))
df = df.withColumn("low_bollinger_band_4d", F.col("moving_avg_4d") - 2*F.col("std_dev_4d"))

df = df.withColumn("up_bollinger_band_week", F.col("moving_avg_week") + 2*F.col("std_dev_week"))
df = df.withColumn("low_bollinger_band_week", F.col("moving_avg_week") - 2*F.col("std_dev_week"))


df.show()
df.count()

pandas_df = df.toPandas()

plt.figure(figsize=(10, 6))
plt.plot(pandas_df['timestamp'], pandas_df['price'], linestyle='-', color='b', label='Price')
plt.plot(pandas_df['timestamp'], pandas_df['moving_avg_4d'], linestyle='-', color='g', label='4 days Moving Avg')
plt.plot(pandas_df['timestamp'], pandas_df['up_bollinger_band_4d'], linestyle='-', color='r', label='Upper Bollinger Band')
plt.plot(pandas_df['timestamp'], pandas_df['low_bollinger_band_4d'], linestyle='-', color='r', label='Lower Bollinger Band')

# Customize the plot
plt.title('Price over Time')
plt.xlabel('Timestamp')
plt.ylabel('Price')
plt.grid(True)

# Show the plot
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.tight_layout()       # Adjust layout
plt.savefig("price_over_time.png", format='png')
'''