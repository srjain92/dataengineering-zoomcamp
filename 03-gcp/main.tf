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