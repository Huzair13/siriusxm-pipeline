terraform {
  backend "s3" {}
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

#updated
module "redshift_procs" {
  source             = "../../terraform_modules/redshift-procs"
  database           = var.database
  workgroup_name     = var.workgroup_name 
  secret_arn         = var.secret_arn
  procedures = {
    load_sales = {
      sql_path = "${path.module}/sql/etl_load_sales.sql"
      schema   = "etl"
    }
  }
}
