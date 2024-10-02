from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F
import re
import boto3
import os
import argparse


def data_processing_price(df):

    window_spec_week = Window.orderBy(F.col('timestamp')).rowsBetween(-168, 0)

    # Calculate moving average, standard deviation and Bollinger Bands
    df = df.withColumn(
        'moving_avg_week',
        F.when(
            F.avg(F.col('price')).over(window_spec_week).isNull(), F.col('price'))
            .otherwise(F.avg(F.col('price')).over(window_spec_week)).cast(FloatType())
    ) \
    .withColumn(
        'std_dev_week',
        F.when(
            F.stddev(F.col('price')).over(window_spec_week).isNull(), 0)
            .otherwise(F.stddev(F.col('price')).over(window_spec_week)).cast(FloatType())
    ) \
    .withColumn('up_bollinger_band_week', F.col('moving_avg_week') + 2*F.col('std_dev_week')) \
    .withColumn('low_bollinger_band_week', F.col('moving_avg_week') - 2*F.col('std_dev_week')) \
    .cache()
    
    return(df)

def data_to_output_bucket(df, filename, type_computing, s3):

    match type_computing:

        case 'local':
            df.write.format('csv').save(filename)
            s3.upload_file(filename, 'historical-crypto', 'output/' + filename)
            os.remove(filename)

        case 'cloud':
            df.write.format('csv').save('s3://historical-crypto/output/' + filename)

def run_processing(filename, type_computing, s3):

    match = re.search(r'days_(.*?)_202', filename)
    if match:
        acronym = match.group(1)
    else:
        print('No match found')
        return(0)

    match = re.search(r'(.*?)_', filename)
    if match:
        type_data = match.group(1)
    else:
        print('No match found')
        return(0)
    
    spark = SparkSession.builder \
    .appName('Batch processing') \
    .getOrCreate()

    match type_computing:

        case 'local':
            s3.download_file('historical-crypto', 'input/' + filename, filename)
            df = spark.read.option('header','true').csv(filename)
            os.remove(filename)

        case 'cloud':
            df = spark.read.option('header','true').csv('s3://historical-crypto/output/' + filename)
    
    df = df.withColumn('timestamp', F.floor(F.col('timestamp')/1000 )).cache()

    match type_data:

        case 'price':

            df_price = data_processing_price(df)
            df_price.write.format('csv').save(filepath)
            return(df_price)


        case 'volume':

            df_volume = df.select('timestamp' , 'volume')
            df_volume.write.format('csv').save(filepath)
            return(df_volume)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-type')
    args = parser.parse_args()

    if args.type not in ['local','cloud']:
        print('Choose \'local\' (execution on the machine) or \'cloud\' (execution on EMR) as -type argument')


    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('historical-crypto')
    input_bucket = my_bucket.objects.filter(Prefix='input/')

    input_file = []

    for my_bucket_object in input_bucket:

        path = my_bucket_object.key
        file_name = os.path.basename(path)
        input_files.append(file_name)
    
    for file in input_files:

        df = run_processing(file, args.type, s3)

        data_to_output_bucket(df, file, args.type, s3)
    

    for file in input_files:

        copy_source = {
            'Bucket': 'historical-crypto',
            'Key': f'input/{file}'
        }

        s3.meta.client.copy(copy_source, 'historical-crypto', f'processed_files_emr/{file}')

        s3.Object('historical-crypto', f'input/{file}').delete()


