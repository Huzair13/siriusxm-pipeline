resource "aws_glue_job" "job" {
  name     = var.job_name
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = var.job_script_location
    python_version  = var.python_version
  }

  glue_version      = var.glue_version
  number_of_workers = var.job_workers
  worker_type       = var.job_worker_type
  default_arguments = var.job_arguments

  execution_property {
    max_concurrent_runs = var.max_concurrent_runs
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_glue_trigger" "trigger" {
  count    = var.create_trigger ? 1 : 0
  name     = var.trigger_name
  type     = var.trigger_type
  actions {
    job_name = aws_glue_job.job.name
  }
}
