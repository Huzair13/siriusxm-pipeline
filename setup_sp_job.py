import os
import shutil
import argparse
import boto3

def get_aws_account_id():
    try:
        sts = boto3.client('sts')
        return sts.get_caller_identity()['Account']
    except Exception as e:
        print(f"Error getting AWS account ID: {e}")
        return "ACCOUNT_ID"

def get_aws_region():
    try:
        session = boto3.session.Session()
        return session.region_name
    except Exception as e:
        print(f"Error getting AWS region: {e}")
        return "YOUR_AWS_REGION"

def update_tfbackend(job_path, job_name, aws_region):
    tfbackend_path = os.path.join(job_path, 'terraform.tfbackend')
    bucket_name = f"terraform-state-talend-migrations-{aws_region}"
    dynamodb_table_name = f"terraform-state-lock-talend-migration-{aws_region}"
    
    content = f"""bucket         = "{bucket_name}"
key            = "sp-jobs/{job_name}/terraform.tfstate"
region         = "{aws_region}"
encrypt        = true
dynamodb_table = "{dynamodb_table_name}"
"""
    
    with open(tfbackend_path, 'w') as f:
        f.write(content)
    
    print(f"Created/Updated terraform.tfbackend for {job_name}")

def setup_sp_job(job_name):
    # Define paths
    base_path = os.path.dirname(os.path.abspath(__file__))
    template_path = os.path.join(base_path, 'redshift_sp_tf_template')
    job_path = os.path.join(base_path, 'sp_jobs', job_name)

    # Create job directory if it doesn't exist
    os.makedirs(job_path, exist_ok=True)

    # Copy template files
    for file in ['main.tf', 'variables.tf', 'terraform.tfvars']:
        shutil.copy(os.path.join(template_path, file), job_path)

    print(f"SP job '{job_name}' Terraform setup completed. Files copied to {job_path}")

    # Get AWS account ID and region
    aws_account_id = get_aws_account_id()
    aws_region = get_aws_region()

    # Update terraform.tfbackend
    update_tfbackend(job_path, job_name, aws_region)

    # Create SQL directory and sample SQL file
    # sql_dir = os.path.join(job_path)
    # os.makedirs(sql_dir, exist_ok=True)
    # with open(os.path.join(sql_dir, f'{job_name}.sql'), 'w') as f:
    #     f.write(f"-- SQL for {job_name}\n")

    # print(f"Created SQL directory and sample SQL file for {job_name}")

    # Update terraform.tfvars with job-specific values
    tfvars_path = os.path.join(job_path, 'terraform.tfvars')
    with open(tfvars_path, 'r') as f:
        tfvars_content = f.read()
    
    updated_tfvars = tfvars_content.replace('ACCOUNT_ID', aws_account_id)
    # updated_tfvars = updated_tfvars.replace('SP_JOB_NAME', job_name)
    
    with open(tfvars_path, 'w') as f:
        f.write(updated_tfvars)

    print(f"Updated terraform.tfvars for {job_name}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Set up new Redshift SP jobs with Terraform configuration")
    parser.add_argument("job_names", nargs='+', help="Names of the SP jobs to set up")
    args = parser.parse_args()

    for job_name in args.job_names:
        setup_sp_job(job_name)
