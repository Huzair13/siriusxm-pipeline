# One-time setup for CodePipeline and CodeBuild

terraform {
  backend "s3" {}
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

module "artifact_bucket" {
  source      = "git::ssh://git@bitbucket.org/siriusxmpoc/siriusxm-modules.git//s3"
  bucket_name = "glue-job-artifact-store-${data.aws_caller_identity.current.account_id}"
}

resource "aws_iam_role" "codepipeline_role" {
  name = "glue-job-codepipeline-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "codepipeline.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "codepipeline_policy" {
  name = "glue-job-codepipeline-policy"
  role = aws_iam_role.codepipeline_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:GetBucketVersioning",
          "s3:PutObject"
        ]
        Resource = [
          module.artifact_bucket.bucket_arn,
          "${module.artifact_bucket.bucket_arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "codestar-connections:UseConnection"
        ]
        Resource = module.codestar_connection.connection_arn
      },
      {
        Effect = "Allow"
        Action = [
          "codebuild:BatchGetBuilds",
          "codebuild:StartBuild"
        ]
        Resource = [
          module.codebuild_project_plan.project_arn,
          module.codebuild_project_apply.project_arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role" "codebuild_role" {
  name = "glue-job-codebuild-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "codebuild.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "codebuild_policy" {
  name = "glue-job-codebuild-policy"
  role = aws_iam_role.codebuild_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:GetBucketVersioning",
          "s3:PutObject"
        ]
        Resource = [
          module.artifact_bucket.bucket_arn,
          "${module.artifact_bucket.bucket_arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/codebuild/*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "glue:*",
          "s3:*",
          "iam:PassRole",
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:GetRepositoryPolicy",
          "ecr:DescribeRepositories",
          "ecr:ListImages",
          "ecr:DescribeImages",
          "ecr:BatchGetImage"
        ]
        Resource = "*"
      }
    ]
  })
}

module "codestar_connection" {
  source          = "git::ssh://git@bitbucket.org/siriusxmpoc/siriusxm-modules.git//codestar_connection"
  connection_name = "bitbucket-connection"
  provider_type   = "Bitbucket"
}

module "codebuild_project_plan" {
  source           = "git::ssh://git@bitbucket.org/siriusxmpoc/siriusxm-modules.git//codebuild_project"
  project_name     = "glue-job-plan"
  description      = "Plans Glue jobs using Terraform"
  service_role_arn = aws_iam_role.codebuild_role.arn
  buildspec_path   = "buildspec.yml"
  image            = "${data.aws_caller_identity.current.account_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com/cicd-terraform-image:latest"
}

module "codebuild_project_apply" {
  source           = "git::ssh://git@bitbucket.org/siriusxmpoc/siriusxm-modules.git//codebuild_project"
  project_name     = "glue-job-apply"
  description      = "Applies Glue jobs using Terraform"
  service_role_arn = aws_iam_role.codebuild_role.arn
  buildspec_path   = "buildspec_apply.yml"
  image            = "${data.aws_caller_identity.current.account_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com/cicd-terraform-image:latest"
}

resource "aws_codepipeline" "glue_job_pipeline" {
  name     = "glue-job-pipeline"
  role_arn = aws_iam_role.codepipeline_role.arn

  artifact_store {
    location = module.artifact_bucket.bucket_name
    type     = "S3"
  }

  stage {
    name = "Source"

    action {
      name             = "Source"
      category         = "Source"
      owner            = "AWS"
      provider         = "CodeStarSourceConnection"
      version          = "1"
      output_artifacts = ["source_output"]

      configuration = {
        ConnectionArn    = module.codestar_connection.connection_arn
        FullRepositoryId = var.bitbucket_repo_id
        BranchName       = var.branch_name
      }
    }
  }

  stage {
    name = "Plan"

    action {
      name            = "Plan"
      category        = "Build"
      owner           = "AWS"
      provider        = "CodeBuild"
      input_artifacts = ["source_output"]
      output_artifacts = ["plan_output"]
      version         = "1"

      configuration = {
        ProjectName = module.codebuild_project_plan.project_name
      }
    }
  }

  stage {
    name = "Approve"

    action {
      name     = "Approval"
      category = "Approval"
      owner    = "AWS"
      provider = "Manual"
      version  = "1"
    }
  }

  stage {
    name = "Apply"

    action {
      name            = "Apply"
      category        = "Build"
      owner           = "AWS"
      provider        = "CodeBuild"
      input_artifacts = ["plan_output"]
      version         = "1"

      configuration = {
        ProjectName = module.codebuild_project_apply.project_name
      }
    }
  }
}

variable "bitbucket_repo_id" {
  description = "The ID of the Bitbucket repository (e.g., 'username/repo-name')"
  type        = string
}

variable "branch_name" {
  description = "Name of the branch to trigger the pipeline"
  type        = string
  default     = "main"
}
