# Contact: yihao zhong
# Net ID: yz7654

# Import package
import pandas as pd
from pyspark.sql.functions import *

# read file using PySpark
# file path in HDFS

# This is the file path
# Contains files of Ride Share Data from 2019-02 to 2022-06
# sample dataset name: fhvhv_tripdata_2022-06.parquet"
df=spark.read.parquet("/user/yz7654/final_project/input") 

# drop some unused columns, clean up the dataset
cols = ("access_a_ride_flag", "wav_request_flag","wav_match_flag","shared_request_flag","dispatching_base_num", \
    "originating_base_num",  " request_datetime", "on_scene_datetime")
df = df.drop(*cols)

# create a trip time in minutes
df = df.withColumn("tripTimeInMin", floor((col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long"))/60))
df.show(3)

# create date time variables: day, month, year, 
df = df.withColumn("Date", col("pickup_datetime").cast("date"))
df = df.withColumn("Date_Y", year("Date"))
df = df.withColumn("Date_M", month("Date"))
df = df.withColumn("Date_D", dayofmonth("Date"))
df = df.withColumn("Date_W", dayofweek("Date")) # Day of week
df = df.withColumn('Hour',hour(df.pickup_datetime)) 

# rename the ride sharing company name
df = df.withColumn("Company", when(df["hvfhs_license_num"] == "HV0003", "Uber").\
    when(df["hvfhs_license_num"] == "HV0005", "Lyft").otherwise("Others"))

# we add up a sum of fee to a new columns
fee_col = ['base_passenger_fare', 'tolls','bcf','sales_tax', 'congestion_surcharge', 'airport_fee','tips']

df = df.withColumn('total_fee', coalesce(col('base_passenger_fare'), lit(0))+coalesce(col('tolls'), lit(0)) \
     +coalesce(col('bcf'), lit(0)) +coalesce(col('sales_tax'), lit(0)) \
     +coalesce(col('congestion_surcharge'), lit(0))+ coalesce(col('airport_fee'), lit(0))+ coalesce(col('tips'), lit(0)))

df.show(3)      

# drop all unused code
cols = ('hvfhs_license_num', 'request_datetime','driver_pay','PULocationID', 'DOLocationID', 'dropoff_datetime',\
 'base_passenger_fare', 'tolls','bcf','sales_tax', 'congestion_surcharge', 'airport_fee','tips')
df = df.drop(*cols)

print("=== This is the preview of ETL output code===")
df.show(3, False) 


# We try to output a cleaned version of all the data, to one parquet file
# But fail because file is too large and cluster runs always failed
# We asked professor and she said it is fine to reuse the code from ETL to Profiling, and ETL + Profiling to Ana 
# to avoid write out large file and re-read in
#df.write.format("parquet").option("maxRecordsPerFile", 5000).save("/user/yz7654/final_project/etl_output")


#df.repartition(50, "Hour").partitionBy("key")
quit()