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

df = df.withColumn("day_hour_range", \
    when(((df["Hour"] >= 0) & (df["Hour"] < 3)), "0-3").\
    when(((df["Hour"] >= 3) & (df["Hour"] < 6)), "3-6").\
    when(((df["Hour"] >= 6) & (df["Hour"] < 9)), "6-9").\
    when(((df["Hour"] >= 9) & (df["Hour"] < 12)), "9-12").\
    when(((df["Hour"] >= 12) & (df["Hour"] < 15)), "12-15").\
    when(((df["Hour"] >= 15) & (df["Hour"] < 18)), "15-18").\
    when(((df["Hour"] >= 18) &(df["Hour"] < 21)), "18-21")\
    .otherwise("21-24"))

cols = ('hvfhs_license_num', 'request_datetime','driver_pay','PULocationID', 'DOLocationID', 'dropoff_datetime',\
 'base_passenger_fare', 'tolls','bcf','sales_tax', 'congestion_surcharge', 'airport_fee','tips')
df = df.drop(*cols)
print("----finished ETL----")


####### start of the ana_code 1 #######
# We focus on average count on different time index

# over year - month, for different company, the trip time duration information
# count - the basic count, subject to the time index 
# daily avg trip count - how many trip per day
# monthly avg trip count - how many trip per monthly
# YoY comparison: year to year change comparison
# MoM comparison: month to month change comparison 
# weekly: namely Mon - Sunday across time index
# hourly range: 24/3 hours = 8 hourly ranges

# 1. year - month, with company, daily average trip and monthly trip
df_yy_mm_cmp_records = df.groupby("Date_Y","Date_M", "Company").agg(count("tripTimeInMin").alias("Total Trip Count"),\
                        floor(count("tripTimeInMin")/30).alias("Daily Avg Trip Count")\
                        )\
                            .sort(desc("Date_Y"), "Date_M")

print("== yearly monthly company records count")                          
df_yy_mm_cmp_records.show(3, truncate = False)     
df_yy_mm_cmp_records.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/ana_output/records/yy_mm_cmp_records.csv")

# 2. yearly, with company, daily average trip and yearly total trip
df_yy_cmp_records = df.groupby("Date_Y", "Company").agg(count("tripTimeInMin").alias("Total_Trip_Count")\
                        )\
                            .sort(desc("Date_Y"))

print("== yearly company records count")                            
df_yy_cmp_records.show(3, truncate = False)     
df_yy_cmp_records.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/ana_output/records/yy_cmp_records.csv")


# 3. year - month, without company, daily average trip and monthly trip
df_yy_mm_records = df.groupby("Date_Y", "Date_M").agg(count("tripTimeInMin").alias("Total Trip Count"),\
                        floor(count("tripTimeInMin")/30).alias("Daily Avg Trip Count"))\
                            .sort(desc("Date_Y"), "Date_M")\
                            
df_yy_mm_records.show(3, truncate = False)     

# Add a Daily YoY comparison (year to year)
my_window = Window.partitionBy().orderBy("Date_Y", "Date_M")
df_yy_mm_records=df_yy_mm_records.withColumn("prev_value", lag(df_yy_mm_records['Daily Avg Trip Count']).over(my_window))
df_yy_mm_records=df_yy_mm_records.withColumn("Daily Avg MoM Percentage (%)", when(isnull(df_yy_mm_records['Daily Avg Trip Count'] - df_yy_mm_records.prev_value), 0)
                              .otherwise((df_yy_mm_records['Daily Avg Trip Count'] - df_yy_mm_records.prev_value))/df_yy_mm_records.prev_value * 100)
df_yy_mm_records = df_yy_mm_records.drop('prev_value')

print("== yearly monthly records count")    
df_yy_mm_records.show(5)
df_yy_mm_records.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/ana_output/records/yy_mm_records.csv")

# 4. yearly, without company, daily average trip, monthly average trip and yearly total trip count
df_yy_records = df.groupby("Date_Y").agg(count("tripTimeInMin").alias("Total_Trip_Count"))\
                            .sort(desc("Date_Y"))\

# add a monthly average trip count
df_yy_records = df_yy_records.withColumn('Avg Monthly Amount of Trip', when(df_yy_records['Date_Y']=='2019', floor(df_yy_records['Total_Trip_Count']/11)).\
    when(df_yy_records['Date_Y']=='2020', floor(df_yy_records['Total_Trip_Count']/12)).\
    when(df_yy_records['Date_Y']=='2021', floor(df_yy_records['Total_Trip_Count']/12)).\
    when(df_yy_records['Date_Y']=='2022', floor(df_yy_records['Total_Trip_Count']/6)))
# calculate monthly YoY comparion percentage
my_window = Window.partitionBy().orderBy("Date_Y")
df_yy_records=df_yy_records.withColumn("prev_value", lag(df_yy_records['Avg Monthly Amount of Trip']).over(my_window))
df_yy_records=df_yy_records.withColumn("Monthly Avg YoY Percentage (%)", when(isnull(df_yy_records['Avg Monthly Amount of Trip'] - df_yy_records.prev_value), 0)
                              .otherwise((df_yy_records['Avg Monthly Amount of Trip'] - df_yy_records.prev_value))/df_yy_records.prev_value * 100)
df_yy_records = df_yy_records.drop('prev_value')

# add a daily average trip count
df_yy_records = df_yy_records.withColumn('Avg Daily Amount of Trip', when(df_yy_records['Date_Y']=='2019', floor(df_yy_records['Total_Trip_Count']/333)).\
    when(df_yy_records['Date_Y']=='2020', floor(df_yy_records['Total_Trip_Count']/365)).\
    when(df_yy_records['Date_Y']=='2021', floor(df_yy_records['Total_Trip_Count']/365)).\
    when(df_yy_records['Date_Y']=='2022', floor(df_yy_records['Total_Trip_Count']/180)))
                               
# calculate daily YoY comparion percentage
my_window = Window.partitionBy().orderBy("Date_Y")
df_yy_records=df_yy_records.withColumn("prev_value", lag(df_yy_records['Avg Daily Amount of Trip']).over(my_window))
df_yy_records=df_yy_records.withColumn("Daily Avg YoY Percentage (%)", when(isnull(df_yy_records['Avg Daily Amount of Trip'] - df_yy_records.prev_value), 0)
                              .otherwise((df_yy_records['Avg Daily Amount of Trip'] - df_yy_records.prev_value))/df_yy_records.prev_value * 100)
df_yy_records = df_yy_records.drop('prev_value')

print("== yearly records count")  
df_yy_records.show(5)
df_yy_records.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/ana_output/records/yy_records.csv")


# 5. year - week, without company, daily average trip and weekly trip, that is (Mon - Sun)
df_yy_ww_records = df.groupby("Date_Y", "Date_W").agg(count("tripTimeInMin").alias("Total_Trip_Count"))\
                       .sort(desc("Date_Y"), "Date_W")\
                            
#df_yy_ww_records.show(3, truncate = False)     

df_yy_ww_records = df_yy_ww_records.withColumn('Avg Daily Amount of Trip', when(df_yy_ww_records['Date_Y']=='2019', floor(df_yy_ww_records['Total_Trip_Count']/48)).\
    when(df_yy_ww_records['Date_Y']=='2020', floor(df_yy_ww_records['Total_Trip_Count']/52)).\
    when(df_yy_ww_records['Date_Y']=='2021', floor(df_yy_ww_records['Total_Trip_Count']/52)).\
    when(df_yy_ww_records['Date_Y']=='2022', floor(df_yy_ww_records['Total_Trip_Count']/26)))

# Add a Daily YoY comparison (year to year)
my_window = Window.partitionBy().orderBy("Date_Y", "Date_W")
df_yy_ww_records=df_yy_ww_records.withColumn("prev_value", lag(df_yy_ww_records['Avg Daily Amount of Trip']).over(my_window))
df_yy_ww_records=df_yy_ww_records.withColumn("Daily Avg YoY Percentage (%)", when(isnull(df_yy_ww_records['Avg Daily Amount of Trip'] - df_yy_ww_records.prev_value), 0)
                              .otherwise((df_yy_ww_records['Avg Daily Amount of Trip'] - df_yy_ww_records.prev_value))/df_yy_ww_records.prev_value * 100)
df_yy_ww_records = df_yy_ww_records.drop('prev_value')

print("== yearly weekly records count")  
df_yy_ww_records.show(5)
df_yy_ww_records.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/ana_output/records/yy_ww_records.csv")


# 6. year - hour range, without company, daily average trip and weekly trip, that is (Mon - Sun)
df_yy_hh_records = df.groupby("Date_Y", "day_hour_range").agg((count("tripTimeInMin")/3).alias("Hourly Avg trip count"))\
                       .sort(desc("Date_Y"))

print("== yearly hourly for hour range records count")  
df_yy_hh_records.show(5)
df_yy_hh_records.coalesce(1).write.option("header",True).csv("/user/yz7654/final_project/ana_output/records/yy_hh_records.csv")


quit()