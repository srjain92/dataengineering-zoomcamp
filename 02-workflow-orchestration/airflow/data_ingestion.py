from sqlalchemy import create_engine
import pandas as pd
from time import time

# The connection string format: 
# postgresql://[user]:[password]@[hostname]:[port]/[database_name]
# In your Docker setup, the hostname is 'postgres'
# CONN_STR = "postgresql://airflow:airflow@postgres:5432/airflow"
# Change 'postgres' to 'localhost' if you run this script outside Docker
CONN_STR = "postgresql://airflow:airflow@postgres:5432/airflow"

engine = create_engine(CONN_STR)


url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"


# We use iterator=True and chunksize to process 100,000 rows at a time
df_iter = pd.read_csv(url, compression='gzip', iterator=True, chunksize=100000)

try:
    t_start = time()
    # Grab the first chunk to test
    df = next(df_iter)

    # Fix the dates
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    # 'replace' creates the table; we only do this for the first chunk
    df.to_sql(name='green_taxi_data', con=engine, if_exists='replace')

    t_end = time()
    print(f'Inserted the first chunk... took {t_end - t_start:.3f} seconds')

    count = 1
    while True:
    
        t_start = time()
        
        df = next(df_iter)
        
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        
        # 'append' adds to the existing table
        df.to_sql(name='green_taxi_data', con=engine, if_exists='append')
        
        t_end = time()
        count += 1
        print('Inserted another chunk..., took %.3f seconds' % (t_end - t_start))

except StopIteration:
        print(f"All data ingested and it had {count} chunks")
        
