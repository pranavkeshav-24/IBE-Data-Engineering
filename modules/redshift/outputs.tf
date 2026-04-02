output "namespace_name" {
  value = aws_redshiftserverless_namespace.main.namespace_name
}

output "workgroup_name" {
  value = aws_redshiftserverless_workgroup.main.workgroup_name
}

output "workgroup_arn" {
  value = aws_redshiftserverless_workgroup.main.arn
}
