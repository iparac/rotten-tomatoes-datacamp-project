provider "google" {
  project = "quiet-maxim-379023"
  region  = "eu-central1"
  zone    = "eu-central1-c"
}

resource "google_storage_bucket" "auto-expire" {
  name          = "rotten-tomatoes-bucket"
  location      = "EU"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = "rotten_tomatoes_reviews"
  friendly_name               = "rotten"
  description                 = "Rotten Tomatoes movies critics reviews"
  location                    = "EU"


  labels = {
    env = "default"
  }
}

resource "google_bigquery_dataset" "dataset_dbt" {
  dataset_id                  = "rotten_tomatoes_dbt"
  friendly_name               = "rotten-dbt"
  description                 = "Transformed rotten tomatoes data using dbt"
  location                    = "EU"


  labels = {
    env = "default"
  }
}

resource "google_service_account" "bqowner" {
  account_id = "rotten-tomatoes-account"
}

resource "google_project_iam_member" "firestore_owner_binding" {
  project = "quiet-maxim-379023"
  role    = "roles/bigquery.dataOwner"
  member  = "serviceAccount:${google_service_account.bqowner.email}"
}