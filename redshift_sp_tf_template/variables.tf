variable "database" {
  description = "The name of the Redshift database"
  type        = string
}

variable "workgroup_name" {
  description = "The name of the Redshift workgroup"
  type        = string
  default     = null
}

variable "secret_arn" {
  description = "The ARN of the AWS Secrets Manager secret containing the Redshift credentials"
  type        = string
}

variable "sp_job_name" {
  description = "The name of the stored procedure job"
  type        = string
}
