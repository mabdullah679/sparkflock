output "bucket" {
  value = aws_s3_bucket.ingest.bucket
}

output "db_host" {
  value = var.create_rds ? aws_db_instance.this[0].address : null
}

output "db_port" {
  value = var.create_rds ? aws_db_instance.this[0].port : null
}

output "db_password" {
  value     = var.create_rds ? random_password.db[0].result : null
  sensitive = true
}
