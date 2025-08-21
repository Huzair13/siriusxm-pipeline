#!/bin/bash

set -e

# Initialize Terraform with backend config
terraform init -backend-config=terraform.tfbackend

# Create and save the plan to a file
terraform plan -var-file=terraform.tfvars -out=replan.tfplan

# Show the saved plan
terraform show replan.tfplan

# Prompt for approval
read -p "Do you want to apply this plan? Type 'yes' to continue: " approve

if [[ "$approve" == "yes" ]]; then
    terraform apply --auto-approve replan.tfplan
else
    echo "Apply cancelled."
fi
