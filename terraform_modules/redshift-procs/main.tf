locals {
  target_count = length(compact([var.workgroup_name, var.cluster_identifier]))
}

resource "null_resource" "validate" {
  triggers = { ok = local.target_count == 1 ? "ok" : "set_one_target" }
}

resource "aws_redshiftdata_statement" "apply_proc" {
  for_each          = var.procedures
  database          = var.database
  workgroup_name    = var.workgroup_name
  cluster_identifier= var.cluster_identifier
  secret_arn        = var.secret_arn
  sql               = file(each.value.sql_path)
  statement_name    = "apply_${each.key}_${filesha1(file(each.value.sql_path))}"
  with_event        = true
  depends_on        = [null_resource.validate]
}