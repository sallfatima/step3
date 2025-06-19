# define variables
variable "location" {
  type        = string
  default     = "us-central1"
  description = "The project location to use."
}

variable "timezone" {
  type        = string
  default = "Africa/Dakar"
  description = "Timezone in tz database name format"
}

variable "cli_args_per_job" {
  type = map(list(string))
  description = "CLI args for the docker image"
}

variable "cron_minute_addition" {
  type = number
  default = 2
  description = "Integer to add to cron minutes, until the job will start "
}

variable "PROJECT_ID" {
  type        = string
  description = "The project name to use."
}

variable "SERVICE_ACCOUNT_ID" {
  type        = string
  description = "The service account id to use."
}

variable "MAPBOX_TOKEN" {
  description = "Mapbox API token"
  type        = string
}

variable "GOOGLE_TOKEN" {
  description = "Google token"
  type        = string
}

variable "GOOGLE_SECRET" {
  description = "Google secret"
  type        = string
}

variable "ROBOFLOW_KEY" {
  description = "Roboflow API key"
  type        = string
}

variable "ROBOFLOW_PROJECT_NAME" {
  description = "Roboflow project name"
  type        = string
}
