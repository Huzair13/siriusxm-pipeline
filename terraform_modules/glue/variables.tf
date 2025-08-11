variable "job_name" {
  type        = string
  description = "Name of the Glue job"
}

variable "glue_role_arn" {
  type        = string
  description = "ARN of the IAM role for Glue"
}

variable "job_script_location" {
  type        = string
  description = "S3 location of the Glue job script"
}

variable "python_version" {
  type        = string
  description = "Python version for the Glue job"
  default     = "3"
}

variable "glue_version" {
  type        = string
  description = "Glue version to use"
  default     = "3.0"
}

variable "job_workers" {
  type        = number
  description = "Number of workers for the Glue job"
  default     = 2
}

variable "job_worker_type" {
  type        = string
  description = "Worker type for the Glue job"
  default     = "G.1X"
}

variable "job_arguments" {
  type        = map(string)
  description = "Default arguments for the Glue job"
  default     = {}
}

variable "max_concurrent_runs" {
  type        = number
  description = "Maximum number of concurrent runs for the Glue job"
  default     = 1
}

variable "create_trigger" {
  type        = bool
  description = "Whether to create a trigger for the Glue job"
  default     = false
}

variable "trigger_name" {
  type        = string
  description = "Name of the Glue trigger"
  default     = null
}

variable "trigger_type" {
  type        = string
  description = "Type of the Glue trigger"
  default     = "ON_DEMAND"
}
