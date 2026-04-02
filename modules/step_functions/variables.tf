variable "name_prefix" { type = string }
variable "common_tags" { type = map(string) }

variable "step_functions_role_arn" { type = string }
variable "sns_topic_arn" { type = string }

variable "lambda_dedup_check_arn" { type = string }
variable "lambda_readiness_check_arn" { type = string }
variable "lambda_redshift_loader_arn" { type = string }
variable "lambda_mark_success_arn" { type = string }

variable "glue_job_name" { type = string }

variable "processed_bucket_id" { type = string }
variable "quarantine_bucket_id" { type = string }
variable "config_bucket_id" { type = string }
