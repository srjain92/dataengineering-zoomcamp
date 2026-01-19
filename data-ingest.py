import pandas as pd
from sqlalchemy import create_engine

# —————— Parquet file (green trips) ——————
parquet_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
df_trips = pd.read_parquet(parquet_url, engine="pyarrow")
print(f"Trip rows: {len(df_trips)}")

# —————— CSV file (zone lookup) ——————
csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
df_zones = pd.read_csv(csv_url)
print(f"Zone rows: {len(df_zones)}")

# —————— Database connection ——————
engine = create_engine(
    "postgresql+psycopg2://app_user:secretpassword@localhost:5432/app_db"
)

# —————— Load to Postgres ——————

# 1) Trips
df_trips.to_sql(
    "green_tripdata",
    engine,
    if_exists="replace",
    index=False,
    chunksize=100_000,
    method="multi"
)
print("Loaded green_tripdata")

# 2) Zones
df_zones.to_sql(
    "taxi_zone_lookup",
    engine,
    if_exists="replace",
    index=False
)
print("Loaded taxi_zone_lookup")
