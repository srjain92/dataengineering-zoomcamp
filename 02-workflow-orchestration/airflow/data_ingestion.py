from sqlalchemy import create_engine
import pandas as pd
from time import time
import os 
import argparse 

def main(args):

    user = args.user
    password = args.password
    host = args.host
    port = args.port
    db = args.db
    table_name = args.table_name
    url = args.url

    # Connection String
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    csv_name = 'output.csv.gz'
    
    print(f"Downloading data from {url}...")

    # Using os.system to run wget command to download the file
    os.system(f"wget {url} -O {csv_name}")

    # We use iterator=True and chunksize to process 100,000 rows at a time
    df_iter = pd.read_csv(csv_name, compression='gzip', iterator=True, chunksize=100000)

    try:
        t_start = time()
        # Grab the first chunk to test
        df = next(df_iter)

        # Fix the dates
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        # 'replace' creates the table; we only do this for the first chunk
        df.to_sql(name=table_name, con=engine, if_exists='replace')

        t_end = time()
        print(f'Inserted the first chunk... took {t_end - t_start:.3f} seconds')

        count = 1
        while True:
    
            t_start = time()
            
            df = next(df_iter)
            
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            
            # 'append' adds to the existing table
            df.to_sql(name=table_name, con=engine, if_exists='append')
            
            t_end = time()
            count += 1
            print('Inserted another chunk..., took %.3f seconds' % (t_end - t_start))

    except StopIteration:
        print(f"All data ingested and it had {count} chunks")
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # No more hardcoded strings! 
    # If the env var is missing and the flag isn't passed, these will be None.
    parser.add_argument('--user', default=os.getenv('DB_USER'), help='user name for postgres')
    parser.add_argument('--password', default=os.getenv('DB_PASSWORD'), help='password for postgres')
    parser.add_argument('--host', default=os.getenv('DB_HOST'), help='host for postgres')
    parser.add_argument('--port', default=os.getenv('DB_PORT'), help='port for postgres')
    parser.add_argument('--db', default=os.getenv('DB_NAME'), help='database name for postgres')
    
    parser.add_argument('--table_name', required=True, help='name of the table')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    # Optional: Add a safety check to ensure credentials exist before starting
    if not all([args.user, args.password, args.host, args.port, args.db]):
        raise ValueError("Missing database credentials! Provide them via Env Vars or CLI flags.")

    main(args)