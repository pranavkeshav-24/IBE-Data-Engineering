output "dedup_table_name" {
  value = aws_dynamodb_table.dedup.name
}

output "dedup_table_arn" {
  value = aws_dynamodb_table.dedup.arn
}

output "sns_topic_arn" {
  value = aws_sns_topic.failure_alerts.arn
}

output "secretsmanager_secret_arn" {
  value = local.create_redshift_secret ? aws_secretsmanager_secret.redshift_credentials[0].arn : data.aws_secretsmanager_secret.existing_redshift_credentials[0].arn
}

output "secretsmanager_secret_kms_key_id" {
  value = local.create_redshift_secret ? "" : try(data.aws_secretsmanager_secret.existing_redshift_credentials[0].kms_key_id, "")
}
