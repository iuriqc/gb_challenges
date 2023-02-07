terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)

  project = var.project
  region  = var.region
  zone    = var.zone
}

resource "google_composer_environment" "gb-env" {
  name   = "${var.project}-composer"
  region = var.region
  config {
    node_count = 3
    node_config {
      zone         = var.zone
      machine_type = "n1-standard-1"
      disk_size_gb = 30
    }
    database_config {
      machine_type = "db-n1-standard-2"
    }
    web_server_config {
      machine_type = "composer-n1-webserver-2"
    }
    software_config {
      image_version = "composer-1.20.4-airflow-2.4.3"
      python_version = "3"
      pypi_packages = {
        pandas = ""
        pandas_gbq = ""
        openpyxl = ""
        requests = ""
        beautifulsoup4 = ""
        tweepy = ""
        apache-airflow-providers-google = ""
      }
    }
  }
}

resource "google_bigquery_dataset" "raw-dataset" {
dataset_id = "RAW"
location = var.region
}

resource "google_bigquery_dataset" "standardized-dataset" {
dataset_id = "STANDARDIZED"
location = var.region
}

resource "google_bigquery_dataset" "curated-dataset" {
dataset_id = "CURATED"
location = var.region
}
