# LOCAL: the AWS provider talks to MinIO instead of AWS.
variable "s3_endpoint" { default = "http://localhost:29000" }
variable "s3_access_key" { default = "sparkflock" }
variable "s3_secret_key" {
  # no default: `make up` passes S3_SECRET_KEY from .env as TF_VAR_s3_secret_key
  sensitive = true
}

terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = var.s3_access_key
  secret_key                  = var.s3_secret_key
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_requesting_account_id  = true
  skip_metadata_api_check     = true
  endpoints {
    s3 = var.s3_endpoint
  }
}

module "pipeline" {
  source      = "../../modules/pipeline"
  bucket_name = "sparkflock-ingest"
  create_rds  = false
}

output "bucket" {
  value = module.pipeline.bucket
}
