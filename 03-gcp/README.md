# Week 2 Homework: Data Warehouse and BigQuery

This directory contains the SQL queries used to analyze the NYC Taxi dataset for the year 2020 and early 2021.

## Dataset Statistics

### Question 3: Yellow Taxi Rows (Year 2020)
*Total count of Yellow taxi trips that occurred in 2020.*

```sql
SELECT count(1)
FROM `nyc-taxi-data-pipeline-485822.nyc_taxi_dataset.yellow_taxi_data` 
WHERE tpep_pickup_datetime >= '2020-01-01'
AND tpep_pickup_datetime < '2021-01-01'; 
'''

### Question 4: Green Taxi 2020 Row Count
**Query:** How many rows are there for the Green Taxi data for all CSV files in the year 2020?

```sql
SELECT 
    count(1) AS total_rows
FROM 
    `nyc-taxi-data-pipeline-485822.nyc_taxi_dataset.green_taxi_data`
WHERE 
    lpep_pickup_datetime >= '2020-01-01'
    AND lpep_pickup_datetime < '2021-01-01';

### Question 5: Yellow Taxi March 2021 Row Count
**Query:** How many rows are there for the Yellow Taxi data for the March 2021 CSV file?

```sql
SELECT 
    count(1) AS march_2021_total
FROM 
    `nyc-taxi-data-pipeline-485822.nyc_taxi_dataset.yellow_taxi_data`
WHERE 
    tpep_pickup_datetime >= '2021-03-01'
    AND tpep_pickup_datetime < '2021-04-01';
