import socket
import threading
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, FloatType
import pyspark.sql.functions as F
from redistimeseries.client import Client




if __name__ == "__main__":

    sc = SparkContext(appName="PythonStreamingNetworkWordCount")
    ssc = StreamingContext(sc, 30)
    
    spark = SparkSession.builder \
    .appName("Crypto Data Stream") \
    .getOrCreate()
    
    lines = ssc.socketTextStream("localhost",9999)

    schema = StructType([
        StructField("acronym", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price", StringType(), True),
        StructField("volume", StringType(), True),
        StructField("extraction_time", StringType(), True)
    ])

    #df = spark.createDataFrame([Row(**i) for i in lines])

    # Function to convert RDD to DataFrame
    def process_rdd(rdd):
        if not rdd.isEmpty():
            data = rdd.collect()
            #print("Received data:", data)
            
            df = spark.read.json(rdd, schema=schema)

            df = df.withColumn(
                "volume_float",
                F.expr("substring(volume, 1, length(volume) - 1)")
            )

            df = df.withColumn(
                "volume",
                F.when(F.expr("substring(volume, length(volume), 1)") == 'M', F.col("volume_float").cast("float") * 1000000 )
                .when(F.expr("substring(volume, length(volume), 1)") == 'B', F.col("volume_float").cast("float") * 1000000000 )
                .when(F.expr("substring(volume, length(volume), 1)") == 'T', F.col("volume_float").cast("float") * 1000000000000 )
                .otherwise(F.regexp_replace("volume", ",", ""))
            )

            df = df.withColumn(
                "volume",
                F.col("volume").cast(FloatType())
            )

            df = df.withColumn(
                "price",
                F.regexp_replace("price", ",", "").cast(FloatType())
            )

            df = df.withColumn(
                "timestamp_minute_floor",
                F.concat(F.expr("substring(extraction_time, 1, length(extraction_time) - 2)") , F.lit("00"))
            )

            timestamp_format = "yyyy-MM-dd HH-mm-ss"
            df = df.withColumn(
                "timestamp_epoch",
                F.unix_timestamp("timestamp_minute_floor", timestamp_format)
            )

            df = df.withColumn(
                "label_volume",
                F.concat(F.lit("LIVE_VOLUME:") , F.col("acronym"))
            )

            df = df.withColumn(
                "label_price",
                F.concat(F.lit("LIVE_PRICE:") , F.col("acronym"))
            )

            #df.show(truncate=False)
            df_price = df.select("label_price", "timestamp_epoch" , "price")
            df_volume = df.select("label_volume", "timestamp_epoch" , "volume")

            rts = Client(host='127.0.0.1', port=6379)

            list_price = [tuple(row) for row in df_price.collect()]
            list_volume = [tuple(row) for row in df_volume.collect()]

            rts.madd(list_price)
            rts.madd(list_volume)

            print("Data imported")

    # Convert each RDD in the DStream to a DataFrame and process it
    lines.foreachRDD(process_rdd)

    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate