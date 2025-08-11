variable "aws_region" {
  description = "The AWS region to deploy resources"
  type        = string
}

variable "common_tags" {
  description = "Common tags to be applied to all resources"
  type        = map(string)
  default     = {}
}

variable "utils_bucket_name" {
  description = "The name of the S3 bucket for utility files"
  type        = string
}

variable "job_name" {
  description = "The name of the Glue job"
  type        = string
}

variable "python_version" {
  description = "The Python version to use for the Glue job"
  type        = string
  default     = "3.9"
}

variable "description" {
  description = "A description of the Glue job"
  type        = string
  default     = ""
}

variable "default_arguments" {
  description = "Default arguments for the Glue job"
  type        = map(string)
  default     = {}
}

variable "glue_role_arn" {
  description = "The ARN of the IAM role for Glue jobs"
  type        = string
}

variable "create_trigger" {
  description = "Whether to create a trigger for the Glue job"
  type        = bool
  default     = false
}

variable "trigger_type" {
  description = "The type of trigger to create (if create_trigger is true)"
  type        = string
  default     = "ON_DEMAND"
}

variable "glue_assets_bucket" {
  description = "The name of the S3 bucket for Glue assets"
  type        = string  
}

variable "utils_requirements_bucket_name" {
  description = "The name of the S3 bucket for utility requirements files"
  type        = string
}