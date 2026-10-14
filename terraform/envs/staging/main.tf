# STAGING: real AWS (free-credits account). Credentials come from your normal
# AWS profile / `aws login`; nothing secret lives in this file.
variable "region" { default = "us-east-1" }
variable "bucket_name" {
  description = "Globally unique, e.g. sparkflock-ingest-<your-initials>-staging"
  type        = string
}
variable "allowed_cidr" {
  description = "Your public IP as x.x.x.x/32"
  type        = string
}

terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = "sparkflock", env = "staging" }
  }
}

module "pipeline" {
  source       = "../../modules/pipeline"
  bucket_name  = var.bucket_name
  create_rds   = true
  allowed_cidr = var.allowed_cidr
}

output "bucket" { value = module.pipeline.bucket }
output "db_host" { value = module.pipeline.db_host }
output "db_port" { value = module.pipeline.db_port }
output "db_password" {
  value     = module.pipeline.db_password
  sensitive = true
}
