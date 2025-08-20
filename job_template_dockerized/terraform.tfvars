aws_region       = "us-east-1"
unified_bucket_name = "default-unified-bucket"
glue_role_arn    = "arn:aws:iam::ACCOUNT_ID:role/Appl-edo-dps-mt-cv-GlueServiceRole"
create_trigger   = false
trigger_type     = "ON_DEMAND"
common_tags      = {
  Environment = "dev"
  Project     = "GlueJobs"
}
python_version = "3.9"
