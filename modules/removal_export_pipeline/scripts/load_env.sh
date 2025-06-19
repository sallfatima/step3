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

# Path to the secret .tfvars file
TFVARS_FILE="./terraform/secrets.auto.tfvars"

# Create or empty the .tfvars file
true > $TFVARS_FILE

# Append variables to the secret .tfvars file
{
    echo "MAPBOX_TOKEN = \"$MAPBOX_TOKEN\""
    echo "GOOGLE_TOKEN = \"$GOOGLE_TOKEN\""
    echo "GOOGLE_SECRET = \"$GOOGLE_SECRET\""
    echo "ROBOFLOW_KEY = \"$ROBOFLOW_KEY\""
    echo "ROBOFLOW_PROJECT_NAME = \"$ROBOFLOW_PROJECT_NAME\""
    echo "PROJECT_ID = \"$PROJECT_ID\""
    echo "PROJECT_NUMBER = \"$PROJECT_NUMBER\""
    echo "SERVICE_ACCOUNT_ID = \"$SERVICE_ACCOUNT_ID\""
    echo "EMAIL_PASSWORD = \"$EMAIL_PASSWORD\""
    echo "EMAIL_SENDER = \"$EMAIL_SENDER\""
} >> $TFVARS_FILE
