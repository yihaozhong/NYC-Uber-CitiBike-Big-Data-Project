# Contact: yihao zhong
# Net ID: yz7654

# Import package
import pandas as pd
from pyspark.sql.functions import *


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

####### START OF THE PROFILING CODE ######
# print the pyspark.dataframe schema
print(df.schema)

# create a hour range in a day
df = df.withColumn("day_hour_range", \
    when(((df["Hour"] >= 0) & (df["Hour"] < 3)), "0-3").\
    when(((df["Hour"] >= 3) & (df["Hour"] < 6)), "3-6").\
    when(((df["Hour"] >= 6) & (df["Hour"] < 9)), "6-9").\
    when(((df["Hour"] >= 9) & (df["Hour"] < 12)), "9-12").\
    when(((df["Hour"] >= 12) & (df["Hour"] < 15)), "12-15").\
    when(((df["Hour"] >= 15) & (df["Hour"] < 18)), "15-18").\
    when(((df["Hour"] >= 18) &(df["Hour"] < 21)), "18-21")\
    .otherwise("21-24"))

# Get raws count
rows = df.count()
print(f"DataFrame Rows count : {rows}")

# Get columns count
cols = len(df.columns)
print(f"DataFrame Columns count : {cols}")

# We divide more into detail

# count the company in different years
df_count_company = df.groupby("Date_Y", "Company").agg(
                        count("Hour").alias("Company Counts over the year"))

df_count_company.show(5)     
df_count_company.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/profiling_output/count_company_yearly.csv")

# count the trip records in different years
df_count_yearly = df.groupby("Date_Y").agg(
                        count("Hour").alias("Yearly Counts"))

df_count_yearly.show(5)
df_count_yearly.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/profiling_output/count_yearly.csv")

# count the trip records in different months across the years
df_count_monthly = df.groupby("Date_M").agg(
                        count("Hour").alias("Months Counts"))

df_count_monthly.show(5)
df_count_monthly.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/profiling_output/count_monthly.csv")

# count the trip records in different hours in a day across these years
df_count_hourly = df.groupby("Hour").agg(
                        count("Company").alias("24 Hours (in a day) Counts"))

df_count_hourly.show(5)
df_count_hourly.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/profiling_output/count_hourly.csv")

# count the trip records in different hour range (8) in a day across these years
df_count_hour_range = df.groupby("day_hour_range").agg(
                        count("Company").alias("Hours Range (in a day) Counts"))

df_count_hour_range.show(5)
df_count_hour_range.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/profiling_output/count_hour_range.csv")

# count the trip records across these years, down to monthly growth
df_count_yy_mm_growth = df.groupby("Date_Y", "Date_M").agg(
                        count("Company").alias("YY_MM Growth Counts"))

df_count_yy_mm_growth.show(5)
df_count_yy_mm_growth.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/profiling_output/yy_mm_growth.csv")


# count the shared ride over the months 
df_count_shared = df.groupby("Date_Y","Date_M", "shared_request_flag").agg(
                        count("Company").alias("Shared Ride Counts"))

df_count_shared.show(5)
df_count_shared.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/profiling_output/count_shared.csv")

# count the trip records in different years
df_count_weekly = df.groupby("Date_W").agg(
                        count("Hour").alias("Weekly Counts"))

df_count_weekly.show(5)
df_count_weekly.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/profiling_output/count_weekly.csv")

df_count_yy_weekly = df.groupby("Date_Y","Date_W").agg(
                        count("Hour").alias("Yearly - Weekly Counts"))

df_count_yy_weekly.show(5)
df_count_yy_weekly.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/profiling_output/count_yy_weekly.csv")


quit()