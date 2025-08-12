module "redshift_procs" {
  source             = "./modules/redshift-procs"
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
