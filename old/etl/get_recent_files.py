from pyspark.sql import SparkSession
from pyspark import SparkContext
import subprocess


#Function to get the n most recent file in HDFS
def get_recent_files(hdfs_path, n, spark):

    #check if there is a spark session
    if not(isinstance(spark, SparkSession)):
        return(None)

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
    return [file[1] for file in files[:n]]

#Stop spark so it does not interfere with other spark instances
#spark.stop()

#Function to retreive a file and the 30 preceiding it
#this function is limited to a range of search of n files (so the file you're looking for need to be within those n files)
def get_n_files_prior(file, n, spark):
    
    files = get_recent_files(file[:38], n+30, spark)

    index_file = files.index(file)

    if index_file >= n:
        return(None)

    return(files[index_file:index_file+30])