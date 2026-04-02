variable "name_prefix" {
  type = string
}

variable "common_tags" {
  type = map(string)
}

# S3 bucket ARNs for policies
variable "raw_bucket_arn" {
  type = string
}

variable "config_bucket_arn" {
  type = string
}

variable "processed_bucket_arn" {
  type = string
}

variable "quarantine_bucket_arn" {
  type = string
}

# DynamoDB / SNS / Secrets Manager
variable "dynamodb_table_arn" {
  type = string
}

variable "sns_topic_arn" {
  type = string
}

variable "secretsmanager_secret_arn" {
  type = string
}

variable "secretsmanager_secret_kms_key_id" {
  description = "KMS key id/arn used by the Redshift credentials secret, if customer-managed."
  type        = string
  default     = ""
}

# Constructed ARNs (to break circular dependencies)
variable "state_machine_arn" {
  description = "Pre-computed State Machine ARN."
  type        = string
}

variable "glue_job_arn" {
  description = "Pre-computed Glue job ARN."
  type        = string
}

variable "lambda_dedup_check_arn" {
  description = "Pre-computed Lambda ARN for dedup check."
  type        = string
}

variable "lambda_readiness_check_arn" {
  description = "Pre-computed Lambda ARN for readiness check."
  type        = string
}

variable "lambda_redshift_loader_arn" {
  description = "Pre-computed Lambda ARN for redshift loader."
  type        = string
}

variable "lambda_mark_success_arn" {
  description = "Pre-computed Lambda ARN for mark success."
  type        = string
}
