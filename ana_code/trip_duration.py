# Contact: yihao zhong
# Net ID: yz7654

# Import package
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.window import Window

####### REUSE OF THE PROFILING CODE ######
df=spark.read.parquet("/user/yz7654/final_project/input") 

# drop some unused columns, clean up the dataset
cols = ("access_a_ride_flag", "wav_request_flag","wav_match_flag","dispatching_base_num", \
    "originating_base_num",  " request_datetime", "on_scene_datetime")
df = df.drop(*cols)

# create a trip time in minutes
df = df.withColumn("tripTimeInMin", floor((col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long"))/60))
# create date time variables: day, month, year, 
df = df.withColumn("Date", col("pickup_datetime").cast("date")).withColumn("Date_Y", year("Date"))\
    .withColumn("Date_M", month("Date")).withColumn("Date_D", dayofmonth("Date"))\
    .withColumn("Date_W", dayofweek("Date"))\
    .withColumn('Hour',hour(df.pickup_datetime)) 


# rename the ride sharing company name
df = df.withColumn("Company", when(df["hvfhs_license_num"] == "HV0003", "Uber").\
    when(df["hvfhs_license_num"] == "HV0005", "Lyft").otherwise("Others"))

df = df.withColumn('total_fee', coalesce(col('base_passenger_fare'), lit(0))+coalesce(col('tolls'), lit(0)) \
     +coalesce(col('bcf'), lit(0)) +coalesce(col('sales_tax'), lit(0)) \
     +coalesce(col('congestion_surcharge'), lit(0))+ coalesce(col('airport_fee'), lit(0))+ coalesce(col('tips'), lit(0)))

cols = ('hvfhs_license_num', 'request_datetime','driver_pay','PULocationID', 'DOLocationID', 'dropoff_datetime',\
 'base_passenger_fare', 'tolls','bcf','sales_tax', 'congestion_surcharge', 'airport_fee','tips')
df = df.drop(*cols)
print("----finished ETL----")


####### start of the ana_code 2 #######

# over year - month, for different company, the trip time duration information
# count - the basic count 
# daily avg trip count - how many trip per day
# daily avg trip duration - how long trip last per daily
# maximum avg trip duration - max of how long trip last per daily
# minimum avg trip duration - always 0

# 1. year - month: with company, avg trip duration
df_yy_mm_cmp_duration = df.groupby("Date_Y","Date_M", "Company").agg(count("tripTimeInMin").alias("Trip Count"),\
                        (avg("tripTimeInMin")).alias("AVG Trip Time (Minutes) M"), \
                        round(max("tripTimeInMin")/60).alias("Max Trip Time (Hours)") \
                        )\
                            .sort(desc("Date_Y"), "Date_M")
                            
df_yy_mm_cmp_duration.show(3, truncate = False)     
df_yy_mm_cmp_duration.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/ana_output/duration/yy_mm_cmp_duration.csv")



# 2. yearly: with company, avg trip duration
df_yy_cmp_duration = df.groupby("Date_Y", "Company").agg(count("tripTimeInMin").alias("Trip Count"),\
                        (avg("tripTimeInMin")).alias("AVG Trip Time (Minutes) Y"), \
                        round(max("tripTimeInMin")/60).alias("Max Trip Time (Hours)") \
                        )\
                            .sort(desc("Date_Y"))
                            
df_yy_cmp_duration.show(3, truncate = False)     
df_yy_cmp_duration.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/ana_output/duration/yy_cmp_duration.csv")

# 3. year - month: without company, avg trip duration
df_yy_mm_duration = df.groupby("Date_Y","Date_M").agg(count("tripTimeInMin").alias("Trip Count"),\
                        (avg("tripTimeInMin")).alias("AVG Trip Time (Minutes) M"), \
                        round(max("tripTimeInMin")/60).alias("Max Trip Time (Hours)") \
                        )\
                            .sort(desc("Date_Y"), "Date_M")
                            
my_window = Window.partitionBy().orderBy("Date_Y", "Date_M")
df_yy_mm_duration=df_yy_mm_duration.withColumn("prev_value", lag(df_yy_mm_duration['AVG Trip Time (Minutes) M']).over(my_window))
df_yy_mm_duration=df_yy_mm_duration.withColumn("Daily Avg MoM Percentage (%)", when(isnull(df_yy_mm_duration['AVG Trip Time (Minutes) M'] - df_yy_mm_duration.prev_value), 0)
                              .otherwise((df_yy_mm_duration['AVG Trip Time (Minutes) M'] - df_yy_mm_duration.prev_value))/df_yy_mm_duration.prev_value * 100)
df_yy_mm_duration = df_yy_mm_duration.drop('prev_value')

df_yy_mm_duration.show(3, truncate = False)     
df_yy_mm_duration.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/ana_output/duration/yy_mm_duration.csv")



# 4. yearly: without company, avg trip duration
df_yy_duration = df.groupby("Date_Y").agg(count("tripTimeInMin").alias("Trip Count"),\
                        (avg("tripTimeInMin")).alias("AVG Trip Time (Minutes) Y"), \
                        round(max("tripTimeInMin")/60).alias("Max Trip Time (Hours)") \
                        )\
                            .sort(desc("Date_Y"))

my_window = Window.partitionBy().orderBy("Date_Y")
df_yy_duration=df_yy_duration.withColumn("prev_value", lag(df_yy_duration['AVG Trip Time (Minutes) Y']).over(my_window))
df_yy_duration=df_yy_duration.withColumn("Daily Avg YoY Percentage (%)", when(isnull(df_yy_duration['AVG Trip Time (Minutes) Y'] - df_yy_duration.prev_value), 0)
                              .otherwise((df_yy_duration['AVG Trip Time (Minutes) Y'] - df_yy_duration.prev_value))/df_yy_duration.prev_value * 100)
df_yy_duration = df_yy_duration.drop('prev_value')

df_yy_duration.show(3, truncate = False)     
df_yy_duration.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/ana_output/duration/yy_duration.csv")


quit()