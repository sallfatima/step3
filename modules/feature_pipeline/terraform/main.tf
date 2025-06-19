# Check version
terraform {
  required_version = ">= 1.3"

  required_providers {
	google = ">= 3.3"
  }
}

# Configure GCP project
provider "google" {
  project = var.PROJECT_ID
}

# Get git SHA2556
data "external" "git" {
  program = [
    "git",
    "log",
    "--pretty=format:{ \"sha\": \"%H\" }",
    "-1",
    "HEAD"
  ]
}

# Set a different Service Account
data "google_service_account" "custom_sa" {
  account_id   = var.SERVICE_ACCOUNT_ID
}

# Set cron time
locals {
  current_time = timestamp()

  # Extract the time components
  year   = substr(local.current_time, 0, 4)
  month  = substr(local.current_time, 5, 2)
  day    = substr(local.current_time, 8, 2)

  # Extract current time components
  current_hour   = tonumber(substr(local.current_time, 11, 2))
  current_minute = tonumber(substr(local.current_time, 14, 2))

  # Calculate future time (5 minutes from now)
  future_minute = local.current_minute + var.cron_minute_addition
  future_hour   = local.current_hour

  # Adjust for overflow of minutes
  adjusted_minute = local.future_minute % 60
  adjusted_hour_1   = local.future_hour + (local.future_minute / 60 < 1 ? 0 : 1)
  adjusted_hour_2   = local.adjusted_hour_1 % 24

  # Convert back to strings with leading zeros if needed
  minute = format("%02d", local.adjusted_minute)
  hour   = format("%02d", local.adjusted_hour_2)

  # Construct the cron expression
  cron = "${local.minute} ${local.hour} ${local.day} ${local.month} *"
}

# Enable Cloud Run API
resource "google_project_service" "run_api" {
  service = "run.googleapis.com"
  disable_on_destroy = false
}

# Define Cloud Run job
resource "google_cloud_run_v2_job" "feature_pipeline_job" {
  for_each = var.cli_args_per_job
  name     = "geomapping-job-${each.key}"
  location = var.location
  deletion_protection = false

  template {
    labels = {
      project_name: "geo-mapping"
    }
    template {
      max_retries = 1
      timeout = "86400s"

      # Define Docker container
      containers {
        image = "${var.location}-docker.pkg.dev/${var.PROJECT_ID}/geomapping/feature_pipeline:${data.external.git.result.sha}"
        args  = each.value

        # Define the environment variables for the container
        env {
          name  = "JOB_NAME"
          value = each.key
        }

        env {
          name  = "MAPBOX_TOKEN"
          value = var.MAPBOX_TOKEN
        }
        env {
          name  = "GOOGLE_TOKEN"
          value = var.GOOGLE_TOKEN
        }
        env {
          name  = "GOOGLE_SECRET"
          value = var.GOOGLE_SECRET
        }
        env {
          name  = "ROBOFLOW_KEY"
          value = var.ROBOFLOW_KEY
        }
        env {
          name  = "ROBOFLOW_PROJECT_NAME"
          value = var.ROBOFLOW_PROJECT_NAME
        }

        # Define the maximum resource limit
        resources {
          limits = {
            cpu    = "8"
            memory = "32000Mi"
          }
        }
      }
    }
  }
  depends_on = [google_project_service.run_api]
}


# Set necessary roles
resource "google_project_iam_member" "run_invoker" {
  project = var.PROJECT_ID
  role    = "roles/run.invoker"
  member  = "serviceAccount:${data.google_service_account.custom_sa.email}"
}

resource "google_project_iam_member" "cloud_scheduler" {
  project = var.PROJECT_ID
  role    = "roles/cloudscheduler.serviceAgent"
  member  = "serviceAccount:${data.google_service_account.custom_sa.email}"
}

# Define Google Cloud Scheduler
resource "google_cloud_scheduler_job" "feature_pipeline_scheduler" {
  for_each    = var.cli_args_per_job
  name        = "geomapping-features-scheduler-${each.key}_v2"
  description = "Run geomapping job ${each.key}"

  region      = var.location
  schedule    = local.cron
  time_zone   = var.timezone

  retry_config {
    retry_count = 3
  }

  http_target {
    http_method = "POST"
    uri         = "https://${google_cloud_run_v2_job.feature_pipeline_job[each.key].location}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.PROJECT_ID}/jobs/${google_cloud_run_v2_job.feature_pipeline_job[each.key].name}:run"

    oauth_token {
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
      service_account_email = data.google_service_account.custom_sa.email
    }
  }
  depends_on = [google_cloud_run_v2_job.feature_pipeline_job]
}
