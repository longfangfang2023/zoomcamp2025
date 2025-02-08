-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `emerald-cacao-368301.zoomcamp.external_yellow_tripdata_2024`
OPTIONS (
  format = 'parquet',
  uris = ['gs://zoomcamp-1000/yellow_tripdata_2024-*.parquet']
);

--Q1
-- Check number of records of yellow trip data
SELECT COUNT(*) FROM emerald-cacao-368301.zoomcamp.external_yellow_tripdata_2024;

--Q2
-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE emerald-cacao-368301.zoomcamp.yellow_tripdata_2024_non_partitioned AS
SELECT * FROM emerald-cacao-368301.zoomcamp.external_yellow_tripdata_2024;

-- check the distinct number of PULocationID in external table
SELECT COUNT(DISTINCT PULocationID) FROM emerald-cacao-368301.zoomcamp.external_yellow_tripdata_2024;


-- check the distinct number of PULocationID in a materialized table
SELECT COUNT(DISTINCT PULocationID) FROM emerald-cacao-368301.zoomcamp.yellow_tripdata_2024_non_partitioned;


--Q3
-- check the distinct number of PULocationID in a materialized table
SELECT COUNT(DISTINCT PULocationID) AS count_PULocation,
       COUNT(DISTINCT DOLocationID) AS count_DOLocation
FROM emerald-cacao-368301.zoomcamp.yellow_tripdata_2024_non_partitioned;


--Q4
-- check the number of records whose fare_amount is 0
SELECT COUNT(*) 
FROM emerald-cacao-368301.zoomcamp.external_yellow_tripdata_2024
WHERE fare_amount = 0;

--Q5
-- Create a partitioned and cluster table from external table
CREATE OR REPLACE TABLE emerald-cacao-368301.zoomcamp.yellow_tripdata_2024_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime) 
CLUSTER BY VendorID AS
SELECT * FROM emerald-cacao-368301.zoomcamp.external_yellow_tripdata_2024;

--Q6
--355ms, 311MB
SELECT COUNT(DISTINCT VendorID) 
FROM emerald-cacao-368301.zoomcamp.yellow_tripdata_2024_non_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15';


--232ms,shuffle 333B,27MB
SELECT COUNT(DISTINCT VendorID) 
FROM emerald-cacao-368301.zoomcamp.yellow_tripdata_2024_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15';