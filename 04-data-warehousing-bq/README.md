## Homework 3: Data Warehousing

### Question 1. What is count of records for the 2024 Yellow Taxi Data?
```sql
SELECT COUNT(1) 
FROM `nyc-taxi-data-pipeline-485822.nyc_taxi_dataset.yellow_taxi_2024_parquet`;
```

### Question 2. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
```sql
SELECT COUNT(DISTINCT PULocationID) 
FROM `nyc-taxi-data-pipeline-485822.nyc_taxi_dataset.external_yellow_taxi_2024_parquet`;

SELECT COUNT(DISTINCT PULocationID) 
FROM `nyc-taxi-data-pipeline-485822.nyc_taxi_dataset.yellow_taxi_2024_parquet`;
```

### Question 3. Why are the estimated number of Bytes different? 
```sql
SELECT PULocationID
FROM `nyc-taxi-data-pipeline-485822.nyc_taxi_dataset.yellow_taxi_2024_parquet`;

SELECT PULocationID, DOLocationID 
FROM `nyc-taxi-data-pipeline-485822.nyc_taxi_dataset.yellow_taxi_2024_parquet`;
```

### Question 4. How many records have a fare_amount of 0? 
```sql
SELECT COUNT(1)
FROM `nyc-taxi-data-pipeline-485822.nyc_taxi_dataset.yellow_taxi_2024_parquet`
WHERE fare_amount = 0;
```

### Question 5. What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID?
```sql
CREATE OR REPLACE TABLE `nyc-taxi-data-pipeline-485822.nyc_taxi_dataset.yellow_taxi_2024_parquet_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `nyc-taxi-data-pipeline-485822.nyc_taxi_dataset.yellow_taxi_2024_parquet`;
```

### Question 6. Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive). Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?
```sql
SELECT DISTINCT VendorID
FROM `nyc-taxi-data-pipeline-485822.nyc_taxi_dataset.yellow_taxi_2024_parquet`
WHERE DATE(tpep_dropoff_datetime) >= '2024-03-01'
AND DATE(tpep_dropoff_datetime) <= '2024-03-15';

SELECT DISTINCT VendorID
FROM `nyc-taxi-data-pipeline-485822.nyc_taxi_dataset.yellow_taxi_2024_parquet_partitioned_clustered`
WHERE DATE(tpep_dropoff_datetime) >= '2024-03-01'
AND DATE(tpep_dropoff_datetime) <= '2024-03-15';
```