aws_region       = "us-east-1"
utils_bucket_name = "talend-migrations-utils-bucket"
glue_assets_bucket = "talend-migrations-glue-assets-bucket"
glue_role_arn    = "arn:aws:iam::253157412491:role/Appl-edo-dps-mt-cv-GlueServiceRole"
create_trigger   = false
trigger_type     = "ON_DEMAND"
common_tags      = {
  Environment = "dev"
  Project     = "GlueJobs"
}
python_version ="3.9"
utils_requirements_bucket_name = "talend-migrations-utils-bucket"
job_name = "job_7"
