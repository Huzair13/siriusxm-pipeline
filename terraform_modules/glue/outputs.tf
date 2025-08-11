output "job_name" {
  value       = aws_glue_job.job.name
  description = "Name of the created Glue job"
}

output "trigger_name" {
  value       = var.create_trigger ? aws_glue_trigger.trigger[0].name : null
  description = "Name of the created Glue trigger (if created)"
}
