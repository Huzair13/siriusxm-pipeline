resource "aws_glue_trigger" "trigger" {
  name     = var.trigger_name
  type     = var.trigger_type
  actions {
    job_name = var.job_name
  }
}

variable "trigger_name" {
  type        = string
  description = "Name of the Glue trigger"
}

variable "trigger_type" {
  type        = string
  description = "Type of the Glue trigger"
  default     = "ON_DEMAND"
}

variable "job_name" {
  type        = string
  description = "Name of the Glue job to trigger"
}

output "trigger_name" {
  value       = aws_glue_trigger.trigger.name
  description = "Name of the created Glue trigger"
}
