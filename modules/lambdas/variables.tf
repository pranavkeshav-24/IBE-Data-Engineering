variable "name_prefix" { type = string }
variable "common_tags" { type = map(string) }

variable "lambdas_source_path" {
  description = "Absolute path to the lambdas/ source directory."
  type        = string
}

variable "build_path" {
  description = "Absolute path to write compiled ZIP archives."
  type        = string
}

variable "lambda_role_arn" { type = string }

# S3
variable "raw_bucket_id" { type = string }
variable "raw_bucket_arn" { type = string }
variable "config_bucket_id" { type = string }
variable "processed_bucket_id" { type = string }
variable "config_prefix" {
  type    = string
  default = "clients"
}
variable "config_index_key" {
  type    = string
  default = "client_config.json"
}

# DynamoDB
variable "dedup_table_name" { type = string }

# Secrets Manager
variable "secretsmanager_secret_arn" { type = string }

# Redshift
variable "redshift_workgroup_name" { type = string }
variable "redshift_database" { type = string }
variable "redshift_s3_role_arn" { type = string }

# Step Functions
variable "state_machine_arn" { type = string }

# Adscribe
variable "adscribe_api_endpoint" { type = string }
variable "adscribe_lookback_days" { type = number }
variable "adscribe_cron_expression" { type = string }

# API
variable "api_allowed_cors_origin" {
  type    = string
  default = "*"
}
