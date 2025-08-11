aws_region       = "us-east-1"
utils_bucket_name = "talend-migration-utils-bucket"
glue_assets_bucket = "talend-migration-glue-assets-bucket"
glue_role_arn    = "arn:aws:iam::ACCOUNT_ID:role/Appl-edo-dps-mt-cv-GlueServiceRole"
create_trigger   = false
trigger_type     = "ON_DEMAND"
common_tags      = {
  Environment = "dev"
  Project     = "GlueJobs"
}
python_version ="3.9"
utils_requirements_bucket_name = "talend-migration-utils-bucket"
job_name = "job_6"
