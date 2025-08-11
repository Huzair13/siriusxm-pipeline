resource "aws_glue_catalog_database" "database" {
  name = var.database_name
}

variable "database_name" {
  type        = string
  description = "Name of the Glue catalog database"
}

output "database_name" {
  value       = aws_glue_catalog_database.database.name
  description = "Name of the created Glue catalog database"
}
