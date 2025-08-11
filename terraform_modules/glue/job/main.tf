resource "aws_glue_job" "job" {
  for_each = var.jobs

  name     = each.key
  role_arn = var.glue_role_arn

  command {
    name            = lookup(each.value, "command_name", "glueetl")
    script_location = each.value.script_location
    python_version  = lookup(each.value, "python_version", "3")
  }

  description       = lookup(each.value, "description", null)
  glue_version      = lookup(each.value, "glue_version", "4.0")
  max_capacity      = lookup(each.value, "max_capacity", null)
  number_of_workers = lookup(each.value, "number_of_workers", null)
  worker_type       = lookup(each.value, "worker_type", "G.1X")
  timeout           = lookup(each.value, "timeout", 2880)

  default_arguments = merge(
    lookup(each.value, "default_arguments", {}),
    var.common_job_arguments
  )

  execution_property {
    max_concurrent_runs = lookup(each.value, "max_concurrent_runs", 1)
  }

  dynamic "notification_property" {
    for_each = lookup(each.value, "notify_delay_after", null) != null ? [1] : []
    content {
      notify_delay_after = each.value.notify_delay_after
    }
  }

  tags = merge(
    var.common_tags,
    lookup(each.value, "tags", {})
  )
}

variable "jobs" {
  type = map(object({
    script_location     = string
    command_name        = optional(string)
    python_version      = optional(string)
    description         = optional(string)
    glue_version        = optional(string)
    max_capacity        = optional(number)
    number_of_workers   = optional(number)
    worker_type         = optional(string)
    timeout             = optional(number)
    default_arguments   = optional(map(string))
    max_concurrent_runs = optional(number)
    notify_delay_after  = optional(number)
    tags                = optional(map(string))
  }))
  description = "Map of Glue job configurations"
}

variable "glue_role_arn" {
  type        = string
  description = "ARN of the IAM role for Glue"
}

variable "common_job_arguments" {
  type        = map(string)
  description = "Common arguments for all Glue jobs"
  default     = {}
}

variable "common_tags" {
  type        = map(string)
  description = "Common tags for all Glue jobs"
  default     = {}
}

output "job_names" {
  value       = [for job in aws_glue_job.job : job.name]
  description = "Names of the created Glue jobs"
}

output "job_arns" {
  value       = [for job in aws_glue_job.job : job.arn]
  description = "ARNs of the created Glue jobs"
}

output "job_details" {
  value = {
    for name, job in aws_glue_job.job :
    name => {
      name               = job.name
      description        = job.description
      role_arn           = job.role_arn
      script_location    = job.command[0].script_location
      python_version     = job.command[0].python_version
      default_arguments  = job.default_arguments
    }
  }
  description = "Detailed information about the created Glue jobs"
}
