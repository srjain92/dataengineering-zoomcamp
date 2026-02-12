## Homework 4: Analytics Engineering

### Question 3. Q3: Count of records in fct_monthly_zone_revenue?
```sql
SELECT count(*) 
FROM `nyc-taxi-data-pipeline-485822.dbt_sjain.fct_monthly_zone_revenue`;
```

### Question 4. Q4: Zone with highest revenue for Green taxis in 2020?
```sql
SELECT pickup_zone
FROM `nyc-taxi-data-pipeline-485822.dbt_sjain.fct_monthly_zone_revenue` 
WHERE service_type = 'Green'
AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY SUM(revenue_monthly_total_amount) DESC
LIMIT 1;
```

### Question 5. Q5: Total trips for Green taxis in October 2019? 
```sql
SELECT SUM(total_monthly_trips)
FROM `nyc-taxi-data-pipeline-485822.dbt_sjain.fct_monthly_zone_revenue` 
WHERE service_type = 'Green'
AND EXTRACT(YEAR FROM revenue_month) = 2019
AND EXTRACT(MONTH FROM revenue_month) = 10;
```

### Question 6. Q6: Count of records in stg_fhv_tripdata (filter dispatching_base_num IS NULL)?