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
REPOSITORY="geomapping"
IMAGE_NAME="inference_pipeline"

# Set other variables
IMAGE_TAG="$(git rev-parse --short HEAD)"
FULL_IMAGE_NAME="${GAR_LOCATION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME}:${IMAGE_TAG}"

# Authenticate to google cloud
echo "Google Authentication through Access Token..."
gcloud auth print-access-token --impersonate-service-account "${ACCOUNT_EMAIL}" | docker login -u oauth2accesstoken --password-stdin https://${GAR_LOCATION}-docker.pkg.dev

# Create the repository if it doesn't exist
echo "Checking if repository exists..."
if ! gcloud artifacts repositories describe "${REPOSITORY}" --location="${GAR_LOCATION}" --project="${PROJECT_ID}" > /dev/null 2>&1; then
    echo "Repository ${REPOSITORY} does not exist. Creating..."
    gcloud artifacts repositories create "${REPOSITORY}" \
        --repository-format=docker \
        --location="${GAR_LOCATION}" \
        --description="Docker repository for ${IMAGE_NAME}" \
        --project="${PROJECT_ID}"
else
    echo "Repository ${REPOSITORY} already exists."
fi

# Build the Docker image
echo "Building Docker image ${FULL_IMAGE_NAME}..."
docker build -t "${FULL_IMAGE_NAME}" -f Dockerfile ../..

# Push the Docker image to Google Artifact Registry
echo "Pushing Docker image to Google Artifact Registry..."
docker push "${FULL_IMAGE_NAME}"
