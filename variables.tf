variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "ap-southeast-1"
}

variable "project_name" {
  description = "Project prefix used for naming resources."
  type        = string
  default     = "momentum-ibe-data"
}

variable "redshift_admin_username" {
  description = "Redshift Serverless admin username."
  type        = string
  default     = "placeholder_name"
}

variable "redshift_admin_password" {
  description = "Redshift Serverless admin password (override in tfvars)."
  type        = string
  sensitive   = true
  default     = "placeholder_pass"
}

variable "redshift_database" {
  description = "Primary Redshift database name."
  type        = string
  default     = "main_db"
}

variable "redshift_schema" {
  description = "Target Redshift schema used by loader and API query Lambda functions."
  type        = string
  default     = "public"
}

variable "redshift_skip_ddl" {
  description = "If true, skip schema/table DDL creation in redshift_loader Lambda."
  type        = bool
  default     = false
}

variable "existing_redshift_secret_name" {
  description = "Optional existing Secrets Manager secret name for Redshift credentials (for example: momentum-ibe-secrets)."
  type        = string
  default     = ""
}

variable "existing_redshift_workgroup_name" {
  description = "Optional existing Redshift Serverless workgroup to use instead of provisioning a new one."
  type        = string
  default     = ""
}

variable "existing_redshift_namespace_name" {
  description = "Optional existing Redshift Serverless namespace name (used for output metadata only)."
  type        = string
  default     = ""
}

variable "adscribe_api_endpoint" {
  description = "Adscribe generate CSV API endpoint."
  type        = string
  default     = "https://i500x8ofql.execute-api.us-east-1.amazonaws.com/prod/generate-csv"
}

variable "adscribe_cron_expression" {
  description = "EventBridge cron expression for daily Adscribe ingestion (UTC)."
  type        = string
  default     = "cron(0 2 * * ? *)"
}

variable "adscribe_lookback_days" {
  description = "Days of backfill the Adscribe fetcher requests per run."
  type        = number
  default     = 1
}

variable "alert_email_subscriptions" {
  description = "Optional SNS email subscriptions for pipeline failure alerts."
  type        = list(string)
  default     = []
}

variable "force_destroy_buckets" {
  description = "Allow terraform destroy to delete non-empty S3 buckets."
  type        = bool
  default     = true
}

variable "api_allowed_cors_origin" {
  description = "CORS origin for API Gateway responses."
  type        = string
  default     = "*"
}

variable "tags" {
  description = "Required tags applied to all supported resources."
  type        = map(string)
  default = {
    "Creator Team" = "Momentum"
    "Purpose"      = "IBE Data Engineering"
  }
}
