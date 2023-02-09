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
      service_account = google_service_account.airflow_service_account
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

resource "google_service_account" "airflow_service_account" {
  account_id = "airflow"
  display_name = "Airflow Service Account"
}

resource "google_project_iam_member" "airflow" {
  member = format("serviceAccount:%s", google_service_account.airflow_service_account)
  role = "roles/composer.worker" # example
}
