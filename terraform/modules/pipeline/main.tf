# One module, two environments. envs/local points the aws provider at MinIO and
# leaves create_rds=false; envs/staging uses real AWS with create_rds=true.

resource "aws_s3_bucket" "ingest" {
  bucket        = var.bucket_name
  force_destroy = true # staging/dev only: lets `terraform destroy` empty the bucket
}

# --- RDS (staging only) ------------------------------------------------------

data "aws_vpc" "default" {
  count   = var.create_rds ? 1 : 0
  default = true
}

resource "random_password" "db" {
  count   = var.create_rds ? 1 : 0
  length  = 24
  special = false
}

resource "aws_security_group" "db" {
  count       = var.create_rds ? 1 : 0
  name_prefix = "${var.bucket_name}-db-"
  description = "Postgres from allowed_cidr only"
  vpc_id      = data.aws_vpc.default[0].id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }
}

resource "aws_db_instance" "this" {
  count                   = var.create_rds ? 1 : 0
  identifier              = "${var.bucket_name}-pg"
  engine                  = "postgres"
  engine_version          = "17"
  instance_class          = var.db_instance_class
  allocated_storage       = 20
  storage_type            = "gp3"
  db_name                 = var.db_name
  username                = var.db_username
  password                = random_password.db[0].result
  vpc_security_group_ids  = [aws_security_group.db[0].id]
  publicly_accessible     = true # staging convenience; pair with a /32 allowed_cidr
  multi_az                = false
  skip_final_snapshot     = true
  backup_retention_period = 1
  apply_immediately       = true
}
