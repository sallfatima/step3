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

# Get short version of git SHA2556
data "external" "git" {
  program = [
    "git",
    "log",
    "--pretty=format:{ \"sha\": \"%h\" }",
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

  # Calculate future time
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

# Create pub/sub topic
resource "google_pubsub_topic" "batch_job_notifications" {
  name = "batch-job-status-updated-removal-export-pipeline-2VV"
}

# # Define bucket
# resource "google_storage_bucket_object" "archive" {
#   name   = "fail_notifications.zip"
#   bucket = "lengo-geomapping"
#   source = "./cloud_functions/fail_notifications.zip"
# }

# # Create cloud function for sending emails
# resource "google_cloudfunctions2_function" "send_failure_email" {
#   name        = "geomapping-removal-export-send-failure-email"
#   description = "Function that sends emails in case of failure"
#   location = var.location
#   build_config {
#     runtime     = "python310"
#     entry_point = "send_email"
#     source {
#       storage_source {
#         bucket = "lengo-geomapping"
#         object = google_storage_bucket_object.archive.name
#       }
#     }
#   }
#   service_config {
#     environment_variables = {
#       EMAIL_SENDER = var.EMAIL_SENDER
#       EMAIL_PASSWORD = var.EMAIL_PASSWORD
#     }
#     service_account_email = data.google_service_account.custom_sa.email
#   }
#
#   event_trigger {
#     trigger_region = var.location
#     event_type = "google.cloud.pubsub.topic.v1.messagePublished"
#     pubsub_topic = google_pubsub_topic.batch_job_notifications.id
#   }
# }

# Give rights to Batch job to publish to a topic
resource "google_pubsub_topic_iam_member" "batch_job_publisher" {
  project = var.PROJECT_ID
  topic   = google_pubsub_topic.batch_job_notifications.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${var.SERVICE_ACCOUNT_ID}@${var.PROJECT_ID}.iam.gserviceaccount.com"
}

resource "google_cloudbuild_trigger" "batch_job_destroy_trigger" {
  for_each = var.cli_args_per_job

  name     = "trigger-duplicate-destroy-${each.key}"
  location = var.location

  service_account = data.google_service_account.custom_sa.id

  pubsub_config  {
    topic = google_pubsub_topic.batch_job_notifications.id
  }

  substitutions = {
    _STATE       = "$(body.message.attributes.NewJobState)"
  }
  filter = "_STATE.matches('SUCCEEDED')"

  build {
    logs_bucket = "gs://lengo-geomapping/cloud_build_logs/removal_export"

    step {
      name       = "gcr.io/google.com/cloudsdktool/cloud-sdk"
      entrypoint = "bash"
      args       = [
        "-c",
        "echo \"Failed to delete job\""
        # "for job_name in $(gcloud batch jobs list --filter='labels.area_name=\"${each.key}\" AND labels.source=\"removal_export_pipeline\"' --sort-by=~createTime --format='value(name)'); do gcloud batch jobs delete \"$job_name\" --location=\"${var.location}\" --quiet || echo \"Failed to delete job $job_name\"; done"
      ]
    }
  }
}

# define a Cloud Scheduler cron job which triggers Batch jobs
resource "google_cloud_scheduler_job" "batch-job-invoker" {
  for_each = var.cli_args_per_job


  name             = "geomapping-removal-export-scheduler-invoker-${each.key}"
  description      = "Run removal_export pipeline job once ${each.key}"
  project          = var.PROJECT_ID
  region           = var.location
  schedule         = local.cron
  time_zone        = var.timezone

  # when this cron job runs, create and run a Batch job
  http_target {
    http_method = "POST"
    uri = "https://batch.googleapis.com/v1/projects/${var.PROJECT_NUMBER}/locations/${var.location}/jobs"
    headers = {
      "Content-Type" = "application/json"
      "User-Agent"   = "Google-Cloud-Scheduler"
    }

    # Batch job definition
    body = base64encode(<<EOT
    {
      "taskGroups":[
        {
          "taskSpec": {
            "runnables":[
              {
                "container": {
                  "imageUri": "${var.location}-docker.pkg.dev/${var.PROJECT_ID}/geomapping/removal_export_pipeline:${data.external.git.result.sha}",
                  "commands": ${jsonencode(each.value)}
                },
                "environment": {
                  "variables": {
                    "MAPBOX_TOKEN": "${var.MAPBOX_TOKEN}",
                    "GOOGLE_TOKEN": "${var.GOOGLE_TOKEN}",
                    "GOOGLE_SECRET": "${var.GOOGLE_SECRET}",
                    "ROBOFLOW_KEY": "${var.ROBOFLOW_KEY}",
                    "ROBOFLOW_PROJECT_NAME": "${var.ROBOFLOW_PROJECT_NAME}"
                  }
                }
              }
            ],
            "computeResource": {
              "cpuMilli": "16000",
              "memoryMib": "104000",
              "bootDiskMib": "200000"
            },
            "maxRetryCount": 0,
            "maxRunDuration": "1209500s"
          }
        }
      ],
      "allocationPolicy": {
        "serviceAccount": {
          "email": "${data.google_service_account.custom_sa.email}"
        },
        "instances": [
          {
            "installGpuDrivers": true,
            "policy": {
              "machineType": "n1-highmem-16",
              "accelerators": [
                {
                  "type": "nvidia-tesla-t4",
                  "count": 1
                }
              ],
            }
          }
        ],
        "location": {
                    "allowedLocations": [
                        "regions/us-central1"
                    ]
        }
      },
      "labels": {
        "project_name": "geo-mapping",
        "source": "removal_export_pipeline",
        "area_name": "${each.key}"
      },
      "logsPolicy": {
        "destination": "CLOUD_LOGGING"
      },
      "notifications": [
        {
          "pubsubTopic": "projects/${var.PROJECT_ID}/topics/${google_pubsub_topic.batch_job_notifications.name}",
          "message": {
            "type": "JOB_STATE_CHANGED",
            "newJobState": "SUCCEEDED"
          }
        },
        {
          "pubsubTopic": "projects/${var.PROJECT_ID}/topics/${google_pubsub_topic.batch_job_notifications.name}",
          "message": {
            "type": "JOB_STATE_CHANGED",
            "newJobState": "FAILED"
          }
        }
      ]
    }
    EOT
    )
    oauth_token {
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
      service_account_email = data.google_service_account.custom_sa.email
    }
  }
}