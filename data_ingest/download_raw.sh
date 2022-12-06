for i in 01 02 03 04 05
do
    curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-$i.parquet
done

for j in 01 02 03 04 05 06 07 08 09 10 11 12
do
    curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-$j.parquet
    curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2020-$k.parquet
done

for i in 02 03 04 05 06 07 08 09 10 11 12 
do
    curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2019-$i.parquet
done
