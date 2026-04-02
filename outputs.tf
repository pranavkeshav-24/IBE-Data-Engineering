output "aws_region" {
  value       = var.aws_region
  description = "Deployment region."
}

output "client_csvs_bucket" {
  value       = module.s3.client_csvs_bucket_id
  description = "Client upload bucket (replicates to raw bucket automatically)."
}

output "raw_bucket" {
  value       = module.s3.raw_bucket_id
  description = "Raw ingestion bucket (S3 events trigger dispatcher Lambda)."
}

output "config_bucket" {
  value       = module.s3.config_bucket_id
  description = "Config bucket (clients/*.json, client_config.json index, Glue script)."
}

output "processed_bucket" {
  value       = module.s3.processed_bucket_id
  description = "Processed Parquet bucket (partitioned by client/date)."
}

output "quarantine_bucket" {
  value       = module.s3.quarantine_bucket_id
  description = "Quarantine bucket for unmatched/invalid rows."
}

output "step_functions_arn" {
  value       = module.step_functions.state_machine_arn
  description = "Ingestion pipeline State Machine ARN."
}

output "glue_job_name" {
  value       = module.glue.glue_job_name
  description = "Glue ETL job name."
}

output "redshift_namespace_name" {
  value = local.create_redshift_serverless ? module.redshift[0].namespace_name : (
    trimspace(var.existing_redshift_namespace_name) != "" ? trimspace(var.existing_redshift_namespace_name) : null
  )
  description = "Redshift namespace name (created by Terraform or provided as existing)."
}

output "redshift_workgroup_name" {
  value       = local.redshift_workgroup_name
  description = "Redshift workgroup name (created by Terraform or provided as existing)."
}

output "api_endpoint" {
  value       = module.api_dashboard.api_endpoint
  description = "API Gateway endpoint URL (prod stage)."
}

output "dashboard_url" {
  value       = module.api_dashboard.dashboard_website_url
  description = "S3 static website URL for the dashboard."
}

output "failure_sns_topic_arn" {
  value       = module.dynamodb_sns.sns_topic_arn
  description = "SNS topic ARN for pipeline failure alerts."
}
