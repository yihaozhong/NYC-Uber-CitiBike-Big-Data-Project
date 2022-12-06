# Surviving dreaded commute at NYC: A case study on Citi Bike &  Ride Hailing

Team: Yihao Zhong (yz7654@nyu.edu), teammate: Yanchen Zhou (yz6956@nyu.edu) <br>
Professor:  Ann Malavet

## Dataset 

- ### Data source

    - TLC websit NY government https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    - Data set usage PDF docs: https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf 


- ### Files locations
    - `/user/yz7654/final_project/input` in HDFS
    - consist of dataset from 2019-02 to 2022-06
    - each month is a dataset, in .parquet format

## Data_Ingestion

- ### Directories
    - ```final_project_yz7654/data_ingest```   

- ### How to fetch data
    - please run the shell executable to fetch data ```final_project_yz7654/data_ingest/download_raw.sh``` using <br>
        ```
        chmod +x download_raw.sh
        ./download_raw.sh
        ```
        We use `curl -o` to fetch the parquet directly into Peel
    - Then we upload all the downloaded dataset from Peel to HDFS. This can takes roughly around 15 minutes. All the datasets are around 20 GB. 
        ```
        hdfs dfs -put final_project/input final_project/input
        ```

## ETL
- ### Directories
    - ```final_project_yz7654/etl_code/Yihao Zhong yz7654``` (local)

- ### Code
    - Path:  ```final_project_yz7654/etl_code/Yihao Zhong yz7654/etl.py``` (local)
    - Path: ```final_project_yz7654/etl_code/Yihao Zhong yz7654/etl.sh``` (local)
    - Clean up (drop) some unused column.
    - Write some new columns that sum up the fee, extract the trip durations, and extract time stamp at different level from the date time variables. 
    - Cut off useless columns for faster run time.

- ### How to run 
    - please run the ```final_project_yz7654/etl_code/etl.sh``` (local) using <br>
        ```
        chmod +x etl.sh
        ./etl.sh
        ```
    
    

- ### Input & Ouput 
    - input: `/user/yz7654/final_project/input` (HDFS)
        - using Spark read.parquet to read all parquet and concat to one dataframe automatically. 

    - output:
        - We tried to output the cleaned dataset to .csv and .parquet but failed because the clusters runs out of memory and the Spark clusters tasks got killed by NYU HPC for long time running. We tried to write to many small files (20k+) but failed for too long execuation time (3h + yet not finished). We tried partition the rows and shrink the size but we are reluctant to shorten our report time. 
        - After commonicate with the professor, she agreed that I decided to give up write out a cleaned dataset. Instead, I just reused the ETL code at the start of Profiling and Analytics so that we avoid huge file write out and read in. So we do not have an 'output' for ETL.

## Profiling 
- ### Directories
    - ```final_project_yz7654/profiling_cod/Yihao Zhong yz7654``` (local)
- ### Code
    - ```final_project_yz7654/profiling_code/Yihao Zhong yz7654/profiling.py``` (local)
    - ```final_project_yz7654/profiling_code/Yihao Zhong yz7654/profiling.sh``` (local)

- ### How to run 
    - please run the ```final_project_yz7654/profiling_code/Yihao Zhong yz7654/profiling.sh```(local) using <br>
        ```
        chmod +x profiling.sh
        ./profiling.sh
        ```

- ### Input & Ouput 
    - input: `/user/yz7654/final_project/input` (HDFS)
        - using Spark read.parquet to read all parquet and concat to one dataframe automatically. 
    - output: `/user/yz7654/final_project/profiling_output`(HDFS)
        - (these are folder names, actual csv are the part-r-...csv inside the folder)
        - count by each year, showing each company, total count in this year.
            - output:  `/profiling_output/count_company_yearly.csv`
        - count by each hour range, the count in each hour range.
            - output:  `/profiling_output/count_hour_range.csv`
        - count by each year, total count in each year
            - output:  `/profiling_output/count_yearly.csv`
        - count by each month, total count in each month
            - output:  `/profiling_output/count_monthly.csv`
        - count by each week day, (Mon - Sun)
            - output:  `/profiling_output/count_weekly.csv`
        - count by each hour of a day.
            - output:  `/profiling_output/count_hourly.csv`
        - count by each year and month, yy - mm
            - output:  `/profiling_output/yy_mm_growth.csv`
        - count by each year andd dayofweek, yy - dayofweek
            - output:  `/profiling_output/count_yy_weekly.csv`
        - count by share ride or not, of yy-mm range
            - output:  `/profiling_output/count_shared.csv`
            
- ### Directories
    - ```final_project_yz7654/ana_code``` (local)
- ### Code
    - ```final_project_yz7654/ana_code/trip_records.py``` (local)
    - ```final_project_yz7654/ana_code/trip_duration.py``` (local)
    - ```final_project_yz7654/ana_code/trip_duration.sh``` (local)
    - ```final_project_yz7654/ana_code/trip_records.sh``` (local)
- ### How to run 
    - please run the two .py in PySpark using <br>
        ```
        chmod +x trip_records.sh
        ./trip_records.sh

        chmod +x trip_duration.sh
        ./trip_duration.sh
        ```

- ### Input & Ouput 
    - input: `/user/yz7654/final_project/input` (HDFS)
        - using Spark read.parquet to read all parquet and concat to one dataframe automatically. 
    - output: `/user/yz7654/final_project/ana_output`(HDFS)
        - `/user/yz7654/final_project/ana_output/records`
            - (these are folder names, csv are the part-r-...csv inside the folder)
            - `yy_cmp_records.csv`
            - `yy_hh_records.csv`
            - `yy_mm_cmp_records.csv`
            - `yy_mm_records.csv`
            - `yy_records.csv`
            - `yy_ww_records.csv`
        - `/user/yz7654/final_project/ana_output/durations`
            - (these are folder names, csv are the part-r-...csv inside the folder)
            - `yy_cmp_duration.csv`
            - `yy_duration.csv`
            - `yy_mm_cmp_duration.csv`
            - `yy_mm_duration.csv`
    - Break down explain:
        - On trip records:
            - `yy_records.csv` answers
                - What is the trend of the overall monthly average trip counts in each year?
                - What is the year to year (YoY) comparison of each year, under the monthly average?
                - What is the trend of the overall daily average trip counts in each year?
                - What is the year to year (YoY) comparison of each year, under daily average?
            - `yy_mm_records.csv` answers
                - What is the trend of the overall daily average trip counts in each months across three years?
                - What is the month to month (MoM) comparison of each months across three years, under daily average?
            - `yy_mm_cmp_records.csv` answers
                - For Uber, Lyft, and Others, what is the trend of the overall daily average trip counts in each months across three years?
            - `yy_cmp_records.csv` answers
                - For Uber, Lyft, and Others, what is the trend of the overall daily average trip counts in each years?
            - `yy_hh_records.csv` answers
                - For each time range (0am - 3am, ..., 9pm - 12 pm, etc), what is the trend of the overall daily average trip counts in each years?
            - `yy_ww_records.csv` answers
                - For each day of week (Mon, ..., Sunday, etc), what is the trend of the overall daily average trip counts in each months across three years?
                - For each day of week (Mon, ..., Sunday, etc), what is the month to month (MoM) comparison in each months across three years?

        - On trip duration
            - trip time is in minutes
            - `yy_mm_duration.csv` answers
                - What is the trend of the overall average trip duration in each months across three years?
                - What is the YoY comparison on average trip duration in each months across three years?
            - `yy_duration.csv`
                - What is the trend of the overall average trip duration in each years?
                - What is the YoY comparison on average trip duration in each months across three years?
            - `yy_cmp_duration.csv`
                - For Uber, Lyft, and Others, what is the trend of the overall average trip duration in each years?
            - `yy_mm_cmp_duration.csv`
                - For Uber, Lyft, and Others, what is the trend of the overall average trip duration in each months across three years?
               

## Screenshots

- profiling_code
    - only show a top few rows of the output to console 
- ana_code
    - only show a top few rows of the output to console
- data_ingest
    - files fetch and upload

## Notes
- run time can be very long
- re-run the code may have the `file already exist` error. If resun, please delete the original output.

