#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
    set -a  # Automatically export all variables
    source .env
    set +a  # Stop exporting all variables
else
    echo ".env not found."
    exit 1
fi

# Define variables
ACCOUNT_EMAIL="${SERVICE_ACCOUNT_ID}@${PROJECT_ID}.iam.gserviceaccount.com"
GAR_LOCATION="us-central1"

# Authenticate to google cloud
echo "Google Authentication through Access Token..."
gcloud auth print-access-token --impersonate-service-account "${ACCOUNT_EMAIL}" | docker login -u oauth2accesstoken --password-stdin https://${GAR_LOCATION}-docker.pkg.dev

echo "Destroying scheduler"
cd terraform && terraform destroy -auto-approve

echo "Destroying batch jobs..."
gcloud batch jobs list --filter labels.source=\"removal_export_pipeline\" --sort-by ~createTime --format="value(name)" | awk -F '/' '{print $NF}' | xargs -I {} gcloud batch jobs delete "{}" --location="${GAR_LOCATION}" --quiet
