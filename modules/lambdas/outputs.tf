output "adscribe_fetcher_arn" {
  value = aws_lambda_function.adscribe_fetcher.arn
}

output "dispatcher_arn" {
  value = aws_lambda_function.dispatcher.arn
}

output "dedup_check_arn" {
  value = aws_lambda_function.dedup_check.arn
}

output "readiness_check_arn" {
  value = aws_lambda_function.readiness_check.arn
}

output "mark_success_arn" {
  value = aws_lambda_function.mark_success.arn
}

output "redshift_loader_arn" {
  value = aws_lambda_function.redshift_loader.arn
}

output "api_query_arn" {
  value = aws_lambda_function.api_query.arn
}

output "api_query_invoke_arn" {
  value = aws_lambda_function.api_query.invoke_arn
}
