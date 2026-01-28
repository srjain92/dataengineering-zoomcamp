FROM python:3.9

# Install system dependencies (wget for downloading the CSV)
RUN apt-get update && apt-get install -y wget

# Install Python libraries for Data Engineering
RUN pip install pandas sqlalchemy psycopg2

# Set the working directory inside the container
WORKDIR /app

# Copy your local script into the container
COPY data_ingestion.py data_ingestion.py

# Define what happens when the container starts
ENTRYPOINT [ "python", "data_ingestion.py" ]