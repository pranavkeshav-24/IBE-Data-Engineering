output "lambda_role_arn" {
  value = aws_iam_role.lambda_role.arn
}

output "step_functions_role_arn" {
  value = aws_iam_role.step_functions_role.arn
}

output "glue_role_arn" {
  value = aws_iam_role.glue_role.arn
}

output "redshift_s3_role_arn" {
  value = aws_iam_role.redshift_s3_role.arn
}
