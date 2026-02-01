terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  # Replace with your actual GCP Project ID
  project = "nyc-taxi-data-pipeline-485822"
  region  = "us-east1"
}

# Create a GCS Bucket (Data Lake)
resource "google_storage_bucket" "demo-bucket" {
  name          = "nyc-taxi-data-pipeline-485822-bucket" # Must be globally unique
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# Create a BigQuery Dataset (Data Warehouse)
resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = "nyc_taxi_dataset"
  location   = "US"
}