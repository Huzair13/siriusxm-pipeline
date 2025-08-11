resource "aws_glue_crawler" "crawler" {
  name          = var.crawler_name
  role          = var.glue_role_arn
  database_name = var.database_name

  s3_target {
    path = var.crawler_s3_target_path
  }
  table_prefix = var.crawler_table_prefix
}

variable "crawler_name" {
  type        = string
  description = "Name of the Glue crawler"
}

variable "glue_role_arn" {
  type        = string
  description = "ARN of the IAM role for Glue"
}

variable "database_name" {
  type        = string
  description = "Name of the Glue catalog database"
}

variable "crawler_s3_target_path" {
  type        = string
  description = "S3 path for the Glue crawler target"
}

variable "crawler_table_prefix" {
  type        = string
  description = "Table prefix for the Glue crawler"
}

output "crawler_name" {
  value       = aws_glue_crawler.crawler.name
  description = "Name of the created Glue crawler"
}
