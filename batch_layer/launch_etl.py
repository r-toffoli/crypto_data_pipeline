from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from get_recent_files import get_n_files_prior
from get_recent_files import get_recent_files
from hdfs_to_hive import load_hdfs_to_hive

start_time_etl = datetime.now()

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


df_load_status = spark.table("default.load_status")
uploaded_file = df_load_status.select("load_status.file_name").rdd.flatMap(lambda x: x).collect()

files = get_recent_files("hdfs://localhost:9000/crypto_data/", 30, spark)
files_to_load = []

for file in files:
    if not(file[34:] in uploaded_file):
        files_to_load.append(file)

#print(files_to_load)
#print(len(files_to_load))

#load_hive(file,spark)

for file in files_to_load:
    file_list = get_n_files_prior(file,30,spark)
    load_hdfs_to_hive(file_list,spark)

spark.stop()

delta_etl = datetime.now() - start_time_etl
print(delta_etl)


#test = get_n_files_prior("hdfs://localhost:9000/crypto_data/crypto_data_2024-09-04_03-33.parquet",30,spark)
#print(test)