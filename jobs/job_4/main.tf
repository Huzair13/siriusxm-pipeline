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


terraform {
  required_version = ">= 1.5.0, < 2.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0" 
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

module "glue_assets_bucket" {
  source    = "../../terraform_modules/s3/s3_objects"
  bucket_id = var.unified_bucket_name

  create_objects = [
    {
      key    = "glue_assets/${local.job_config.script_location}"
      source = "${path.module}/../../${local.job_config.script_location}"
      etag   = filemd5("${path.module}/../../${local.job_config.script_location}")
      tags   = local.common_tags
    }
  ]
}

module "utils_bucket" {
  source    = "../../terraform_modules/s3/s3_objects"
  bucket_id = var.unified_bucket_name
  
  create_objects = [
    {
      key    = "utils/jobs/${var.job_name}/utils-0.1-py3-none-any.whl"
      source = "${path.module}/utils-0.1-py3-none-any.whl"
      etag   = filemd5("${path.module}/utils-0.1-py3-none-any.whl")
      tags   = local.common_tags
    }
  ]
}

module "utils_requirements" {
  source    = "../../terraform_modules/s3/s3_objects"
  bucket_id = var.unified_bucket_name

  create_objects = [
    {
      key    = "utils/jobs/${var.job_name}/requirements.txt"
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
  job_script_location = "s3://${var.unified_bucket_name}/glue_assets/${local.job_config.script_location}"
  python_version      = local.job_config.python_version
  glue_version        = "5.0"
  job_workers         = 2
  job_worker_type     = "G.1X"
  job_arguments = merge(
    local.job_config.default_arguments,
    {
      "--TempDir"                   = "s3://${var.unified_bucket_name}/temp/",
      "--extra-py-files"            = "s3://${var.unified_bucket_name}/utils/jobs/${var.job_name}/utils-0.1-py3-none-any.whl",
      "--additional-python-modules" = "-r,s3://${var.unified_bucket_name}/utils/jobs/${var.job_name}/requirements.txt"
    }
  )
  max_concurrent_runs = 1

  create_trigger = var.create_trigger
  trigger_name   = "${var.job_name}-trigger"
  trigger_type   = var.trigger_type
}
