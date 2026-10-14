variable "bucket_name" {
  description = "Landing bucket for incoming JSON files."
  type        = string
}

variable "create_rds" {
  description = "false locally (Postgres runs in Docker); true in staging."
  type        = bool
  default     = false
}

variable "db_name" {
  type    = string
  default = "sparkflock"
}

variable "db_username" {
  type    = string
  default = "sparkflock"
}

variable "db_instance_class" {
  description = "Free-tier eligible class. Check your account's free plan before changing."
  type        = string
  default     = "db.t4g.micro"
}

variable "allowed_cidr" {
  description = "Who may reach Postgres on 5432 (your public IP as x.x.x.x/32). Never 0.0.0.0/0."
  type        = string
  default     = ""

  validation {
    condition     = var.allowed_cidr != "0.0.0.0/0"
    error_message = "allowed_cidr must not be 0.0.0.0/0."
  }
}
