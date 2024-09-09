from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType
#from get_recent_files import get_recent_files
import pyspark.sql.functions as F

'''
start_time = datetime.now()

# Create a Spark session with Hive support and configuration
# with mysql connector jar, logins to MySql and connection URL
spark = SparkSession.builder \
    .appName("Load data Hive") \
    .enableHiveSupport() \
    .config("spark.driver.extraClassPath", "/opt/spark/spark-3.5.2-bin-hadoop3/jars/mysql-connector-java-8.0.28.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/spark-3.5.2-bin-hadoop3/jars/mysql-connector-java-8.0.28.jar") \
    .config("javax.jdo.option.ConnectionURL", "jdbc:mysql://localhost/metastore?createDatabaseIfNotExist=true&useSSL=false&serverTimezone=UTC") \
    .config("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver") \
    .config("javax.jdo.option.ConnectionUserName", "root") \
    .config("javax.jdo.option.ConnectionPassword", "root") \
    .getOrCreate()

# ---- Extract ----

# List of the 30 most recent files of HDFS
files = get_recent_files("hdfs://localhost:9000/crypto_data/", 30, spark)
'''

def load_hdfs_to_hive(files, spark):

    start_time = datetime.now()

    print("Loading:",files[0])

    # Load multiple DataFrames from Parquet files and concatenate them
    # starting with the first dataframe
    dfs = [spark.read.format("parquet").load(file) for file in files]
    combined_df = dfs[0]

    # Merge all DataFrames
    for df in dfs[1:]:
        combined_df = combined_df.union(df)


    # ---- Transform ----

    #extraction_time to TimeStamp
    combined_df = combined_df.withColumn(
        "extraction_time_ts",
        F.to_timestamp("extraction_time", "yyyy-MM-dd HH-mm-ss"),
    )

    #price to float
    combined_df = combined_df.withColumn(
        "price_f",
        F.regexp_replace("price", ",", "").cast(FloatType())
    )

    #volume (xxxT, xxxB or xxxM initial format) to float
    combined_df = combined_df.withColumn(
        "volume_f",
        F.expr("substring(volume, 1, length(volume) - 1)")
    )

    combined_df = combined_df.withColumn(
        "volume_f",
        F.when(F.expr("substring(volume, length(volume), 1)") == 'M', F.col("volume_f").cast("float") * 1000000 )
        .when(F.expr("substring(volume, length(volume), 1)") == 'B', F.col("volume_f").cast("float") * 1000000000 )
        .when(F.expr("substring(volume, length(volume), 1)") == 'T', F.col("volume_f").cast("float") * 1000000000000 )
        .otherwise(F.regexp_replace("volume", ",", ""))
    )

    combined_df = combined_df.withColumn(
        "volume_f",
        F.col("volume_f").cast(FloatType())
    )

    #combined_df.show()

    # Define window specification for moving average (5,10 and 30 periods)
    window_spec_5 = Window.partitionBy("name").orderBy(F.col("extraction_time_ts")).rowsBetween(-5, 0)
    window_spec_10 = Window.partitionBy("name").orderBy(F.col("extraction_time_ts")).rowsBetween(-10, 0)
    window_spec_30 = Window.partitionBy("name").orderBy(F.col("extraction_time_ts")).rowsBetween(-30, 0)

    # Calculate the moving average
    moving_avg_df = combined_df.withColumn("moving_avg_df_5", F.avg(F.col("price_f")).over(window_spec_5).cast(FloatType()))
    moving_avg_df = moving_avg_df.withColumn("moving_avg_df_10", F.avg(F.col("price_f")).over(window_spec_10).cast(FloatType()))
    moving_avg_df = moving_avg_df.withColumn("moving_avg_df_30", F.avg(F.col("price_f")).over(window_spec_30).cast(FloatType()))

    #moving_avg_df.filter(moving_avg_df['name'] == "Bitcoin").show()

    # ---- Load ----

    #Get the extraction time of the last extracted file
    last_file = files[0]
    extraction_time_last_file = last_file[46:62].replace("_"," ")


    load_df = moving_avg_df['acronym','name','currency','price','extraction_time_ts','price_f','volume_f','moving_avg_df_5','moving_avg_df_10','moving_avg_df_30'] \
    .filter(moving_avg_df['extraction_time'].contains(extraction_time_last_file))

    #Separate the dataframe (one to load in crypto_live_data and crypto_details)
    load_df_live_data = load_df['acronym','extraction_time_ts','currency','price_f','volume_f','moving_avg_df_5','moving_avg_df_10','moving_avg_df_30']
    load_df_details = load_df['acronym','name']

    #Reduce crypto_details dataframe to data not already present in crypto_details Hive
    columns_to_compare = ['acronym','name']
    df_crypto_details = spark.table("default.crypto_details").withColumnRenamed("crypto_acronym", "acronym").withColumnRenamed("crypto_name", "name")
    load_df_details = load_df_details.join(df_crypto_details, on=columns_to_compare, how='left_anti')

    #Load data in Hive
    load_df_details.write.mode("append").insertInto("default.crypto_details")
    load_df_live_data.write.mode("append").insertInto("default.crypto_live_data")

    #Update load_status to account for Parquet file upload in Hive
    df_load_status = spark.createDataFrame([(files[0][34:], "Imported") ], ["file_name", "status"])
    df_load_status.write.mode("append").insertInto("default.load_status")

    print("Done: ",files[0])

    delta = datetime.now() - start_time
    print(delta)