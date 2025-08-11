terraform {
  backend "s3" {}
}

locals {
  job_config = jsondecode(file("${path.module}/config.json"))
  common_tags = {
    Environment = terraform.workspace
    ManagedBy   = "Terraform"
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

module "glue_assets_bucket" {
  source    = "../../terraform_modules/s3/s3_objects"
  bucket_id = var.glue_assets_bucket

  create_objects = [
    {
      key    = local.job_config.script_location
      source = "${path.module}/../../${local.job_config.script_location}"
      etag   = filemd5("${path.module}/../../${local.job_config.script_location}")
      tags   = local.common_tags
    }
  ]
}

# The following blocks were removed because the wheel file is now built by the buildspec.yml
# resource "null_resource" "ensure_setup_py"
# resource "null_resource" "create_utils_whl"
# data "local_file" "utils_whl"

module "utils_bucket" {
  source    = "../../terraform_modules/s3/s3_objects"
  bucket_id = var.utils_bucket_name

  create_objects = [
    {
      # The buildspec now creates this wheel file in the job's directory before Terraform runs.
      key    = "jobs/${var.job_name}/utils-0.1-py3-none-any.whl"
      source = "${path.module}/utils-0.1-py3-none-any.whl"
      etag   = filemd5("${path.module}/utils-0.1-py3-none-any.whl")
      tags   = local.common_tags
    }
  ]
}

module "utils_requirements" {
  source    = "../../terraform_modules/s3/s3_objects"
  bucket_id = var.utils_bucket_name

  create_objects = [
    {
      key    = "jobs/${var.job_name}/requirements.txt"
      source = "${path.module}/requirements.txt"
      etag   = filemd5("${path.module}/requirements.txt")
      tags   = local.common_tags
    }
  ]
}

# Glue Job and Trigger
module "glue_job" {
  source = "../../terraform_modules/glue"

  job_name            = var.job_name
  glue_role_arn       = var.glue_role_arn
  job_script_location = "s3://${var.glue_assets_bucket}/${local.job_config.script_location}"
  python_version      = local.job_config.python_version
  glue_version        = "5.0"
  job_workers         = 2
  job_worker_type     = "G.1X"
  job_arguments = merge(
    local.job_config.default_arguments,
    {
      "--TempDir"                   = "s3://${var.glue_assets_bucket}/temp/",
      "--extra-py-files"            = "s3://${var.utils_bucket_name}/jobs/${var.job_name}/utils-0.1-py3-none-any.whl",
      "--additional-python-modules" = "-r,s3://${var.utils_bucket_name}/jobs/${var.job_name}/requirements.txt"
    }
  )
  max_concurrent_runs = 1

  create_trigger = var.create_trigger
  trigger_name   = "${var.job_name}-trigger"
  trigger_type   = var.trigger_type
}
