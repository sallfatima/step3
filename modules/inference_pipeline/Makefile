# Define the phony targets that are just simple commands
.PHONY: init plan apply load_env build_push destroy

# Build with Docker and push to GAR
build_push:
	@bash ./scripts/build_push.sh

# Call the secret environment variable script
load_env:
	@bash ./scripts/load_env.sh

# Initialize Terraform
init:
	cd terraform && terraform init

# Create a Terraform execution plan and build and push the docker image
plan: load_env build_push
	cd terraform && terraform plan

# Apply Terraform configuration
apply: load_env
	cd terraform && terraform apply -auto-approve

# Destroy terraform resources
destroy: load_env
	cd terraform && terraform destroy -auto-approve
