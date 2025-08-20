import os
import shutil
import json
import argparse
import boto3
import zipfile
import re
import boto3

import subprocess
import sys

def get_aws_account_id():
    try:
        sts = boto3.client('sts')
        return sts.get_caller_identity()['Account']
    except Exception as e:
        print(f"Error getting AWS account ID: {e}")
        return "ACCOUNT_ID"

def create_empty_zip(zip_path):
    with zipfile.ZipFile(zip_path, 'w') as empty_zip:
        pass  # Create an empty ZIP file

def ensure_setup_py(job_path, job_name='glue-job'):
    """
    Creates a minimal setup.py in job_path if it doesn't exist.
    """
    setup_py = os.path.join(job_path, "setup.py")
    if not os.path.exists(setup_py):
        with open(setup_py, "w") as f:
            f.write(f"""
from setuptools import setup, find_packages

setup(
    name='utils',
    version='0.1',
    packages=find_packages(),
    description='Glue utils for {job_name}',
    include_package_data=True,
    package_data={{'utils': ['vendor/**/*']}},
)
""")
        print(f"Created setup.py in {job_path}")
    else:
        print(f"setup.py already exists in {job_path}, skipping.")


def create_utils_zip(src_folder, zip_path):
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for root, dirs, files in os.walk(src_folder):
            for file in files:
                file_path = os.path.join(root, file)
                # This will keep 'utils/filename' structure in the zip
                arcname = os.path.relpath(file_path, os.path.dirname(src_folder))
                zipf.write(file_path, arcname)


def create_utils_wheel(job_dir, wheel_output_folder, job_name):
    import importlib.util
    import subprocess
    import sys
    import shutil
    import os

    # Ensure wheel_output_folder exists
    os.makedirs(wheel_output_folder, exist_ok=True)

    # Clean up old build artifacts
    for artifact in ["build", "dist", "utils.egg-info"]:
        artifact_path = os.path.join(job_dir, artifact)
        if os.path.exists(artifact_path):
            shutil.rmtree(artifact_path)

    # Check for setuptools and wheel, try to install if missing
    for package in ['setuptools', 'wheel']:
        if importlib.util.find_spec(package) is None:
            try:
                subprocess.check_call([sys.executable, "-m", "ensurepip", "--upgrade"])
            except Exception as e:
                print(f"Warning: ensurepip failed: {e}")
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", package])
            except Exception as e:
                print(f"Failed to install {package}. Please install it manually.")
                raise

    # Ensure setup.py exists in job_dir
    setup_py = os.path.join(job_dir, "setup.py")
    if not os.path.exists(setup_py):
        ensure_setup_py(job_dir, job_name)

    # Build the wheel
    subprocess.check_call([
        sys.executable, "setup.py", "bdist_wheel", "--dist-dir", wheel_output_folder
    ], cwd=job_dir)
    print(f"Created .whl bundle in {wheel_output_folder}")


def generate_valid_bucket_name(job_name):
    bucket_name = re.sub(r'[^a-zA-Z0-9-]', '-', job_name.lower())
    if not bucket_name[0].isalnum():
        bucket_name = 'a' + bucket_name
    return f"{bucket_name[:59]}-utils"[:63]

def get_aws_region():
    try:
        session = boto3.session.Session()
        return session.region_name
    except Exception as e:
        print(f"Error getting AWS region: {e}")
        return "YOUR_AWS_REGION"

def update_tfbackend(job_path, job_name, aws_region):
    tfbackend_path = os.path.join(job_path, 'terraform.tfbackend')
    bucket_name = f"presidio-jobs-tf-state"
    dynamodb_table_name = f"terraform-state-lock-talend-migration"
    
    content = f"""bucket         = "{bucket_name}"
key            = "glue-jobs/{job_name}/terraform.tfstate"
region         = "{aws_region}"
encrypt        = true
dynamodb_table = "{dynamodb_table_name}"
"""
    
    with open(tfbackend_path, 'w') as f:
        f.write(content)
    
    print(f"Created/Updated terraform.tfbackend for {job_name}")

def setup_glue_job(job_name):
    # Define paths
    base_path = os.path.dirname(os.path.abspath(__file__))
    template_path = os.path.join(base_path, 'job_template_dockerized')
    job_path = os.path.join(base_path, 'jobs', job_name)

    # Create job directory if it doesn't exist
    os.makedirs(job_path, exist_ok=True)

    # Copy template files
    for file in ['main.tf', 'variables.tf', 'terraform.tfvars', 'terraform.tfbackend','dockerfile','deploy.sh']:
        shutil.copy(os.path.join(template_path, file), job_path)

    print(f"Glue job '{job_name}' Terraform setup completed. Files copied to {job_path}")

    # Get AWS account ID and region
    aws_region = get_aws_region()

    # Update terraform.tfbackend
    update_tfbackend(job_path, job_name, aws_region)

    # Create job-specific config.json
    job_config = {
        "name": job_name,
        "script_location": f"jobs/{job_name}/main.py",
        "python_version": "3.9",
        "description": f"Glue job for {job_name}",
        "default_arguments": {
            "--job-language": "python"
        }
    }

    config_path = os.path.join(job_path, 'config.json')
    with open(config_path, 'w') as f:
        json.dump(job_config, f, indent=2)

    print(f"Created config.json for {job_name}")

    utils_src_folder = os.path.join(job_path, 'src')
    utils_zip_path = os.path.join(job_path, 'utils.zip')

    job_path = os.path.join(base_path, 'jobs', job_name)
    if os.path.exists(os.path.join(job_path, 'src')):
        create_utils_wheel(job_path, job_path, job_name)

    else:
        print(f"No utils folder found for {job_name}. Skipping wheel creation.")

    tfvars_path = os.path.join(job_path, 'terraform.tfvars')
    with open(tfvars_path, 'r') as f:
        tfvars_content = f.read()
    
    aws_account_id = get_aws_account_id()

    unified_bucket = "talend-migrations-unified-bucket"
    updated_tfvars = tfvars_content.replace('default-unified-bucket', unified_bucket)
    updated_tfvars = updated_tfvars.replace('ACCOUNT_ID', aws_account_id)
    updated_tfvars += f'\njob_name = "{job_name}"\n'
    
    with open(tfvars_path, 'w') as f:
        f.write(updated_tfvars)

    print(f"Updated terraform.tfvars for {job_name}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Set up new AWS Glue jobs with Terraform configuration")
    parser.add_argument("job_names", nargs='+', help="Names of the Glue jobs to set up")
    args = parser.parse_args()

    for job_name in args.job_names:
        setup_glue_job(job_name)
