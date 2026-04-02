# ------------------------------------------------------------------------------
# DynamoDB (dedup), SNS (failure alerts), Secrets Manager (Redshift creds)
# ------------------------------------------------------------------------------

resource "aws_dynamodb_table" "dedup" {
  name         = "${var.name_prefix}-dedup"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "file_hash"

  attribute {
    name = "file_hash"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  tags = merge(var.common_tags, { Name = "${var.name_prefix}-dedup" })
}

resource "aws_sns_topic" "failure_alerts" {
  name = "${var.name_prefix}-pipeline-failures"
  tags = merge(var.common_tags, { Name = "${var.name_prefix}-pipeline-failures" })
}

resource "aws_sns_topic_subscription" "failure_emails" {
  for_each  = toset(var.alert_email_subscriptions)
  topic_arn = aws_sns_topic.failure_alerts.arn
  protocol  = "email"
  endpoint  = each.value
}

locals {
  create_redshift_secret = trimspace(var.existing_redshift_secret_name) == ""
}

data "aws_secretsmanager_secret" "existing_redshift_credentials" {
  count = local.create_redshift_secret ? 0 : 1
  name  = trimspace(var.existing_redshift_secret_name)
}

resource "aws_secretsmanager_secret" "redshift_credentials" {
  count       = local.create_redshift_secret ? 1 : 0
  name        = "${var.name_prefix}/redshift/admin"
  description = "Redshift admin credentials for pipeline loaders and dashboard API."
  tags        = merge(var.common_tags, { Name = "${var.name_prefix}-redshift-credentials" })
}

resource "aws_secretsmanager_secret_version" "redshift_credentials" {
  count     = local.create_redshift_secret ? 1 : 0
  secret_id = aws_secretsmanager_secret.redshift_credentials[0].id
  secret_string = jsonencode({
    username = var.redshift_admin_username
    password = var.redshift_admin_password
    dbname   = var.redshift_database
  })
}
