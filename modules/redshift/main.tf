# ------------------------------------------------------------------------------
# Redshift Serverless — Namespace + Workgroup (default VPC)
# ------------------------------------------------------------------------------

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = replace("${var.name_prefix}ns", "-", "")
  db_name             = var.redshift_database
  admin_username      = var.redshift_admin_username
  admin_user_password = var.redshift_admin_password
  iam_roles           = [var.redshift_s3_role_arn]

  tags = merge(var.common_tags, { Name = "${var.name_prefix}-namespace" })
}

resource "aws_redshiftserverless_workgroup" "main" {
  workgroup_name      = replace("${var.name_prefix}wg", "-", "")
  namespace_name      = aws_redshiftserverless_namespace.main.namespace_name
  base_capacity       = 8
  publicly_accessible = false
  subnet_ids          = var.subnet_ids

  tags = merge(var.common_tags, { Name = "${var.name_prefix}-workgroup" })
}
