aws_region       = "us-east-1"
unified_bucket_name = "talend-migrations-unified-bucket"
glue_role_arn    = "arn:aws:iam::253157412491:role/Appl-edo-dps-mt-cv-GlueServiceRole"
create_trigger   = false
trigger_type     = "ON_DEMAND"
common_tags      = {
  Environment = "dev"
  Project     = "GlueJobs"
}
python_version = "3.9"

job_name = "job_1"
