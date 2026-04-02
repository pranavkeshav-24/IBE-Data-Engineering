output "api_endpoint" {
  value       = "${aws_apigatewayv2_api.dashboard.api_endpoint}/${aws_apigatewayv2_stage.prod.name}"
  description = "API Gateway endpoint (regional)."
}

output "apigw_execution_arn" {
  value       = aws_apigatewayv2_api.dashboard.execution_arn
  description = "Execution ARN used for Lambda permission."
}

output "dashboard_website_url" {
  value       = "http://${aws_s3_bucket_website_configuration.dashboard.website_endpoint}"
  description = "S3 static website URL for the dashboard."
}

output "dashboard_bucket_id" {
  value = aws_s3_bucket.dashboard.id
}
