# ------------------------------------------------------------------------------
# API Dashboard — API Gateway HTTP + S3 Static Website (no CloudFront)
# ------------------------------------------------------------------------------

# --- API Gateway HTTP API ---

resource "aws_apigatewayv2_api" "dashboard" {
  name          = "${var.name_prefix}-dashboard-api"
  protocol_type = "HTTP"

  cors_configuration {
    allow_headers = ["content-type", "authorization"]
    allow_methods = ["GET", "POST", "OPTIONS"]
    allow_origins = [var.api_allowed_cors_origin]
    max_age       = 300
  }

  tags = merge(var.common_tags, { Name = "${var.name_prefix}-dashboard-api" })
}

resource "aws_apigatewayv2_integration" "dashboard_lambda" {
  api_id                 = aws_apigatewayv2_api.dashboard.id
  integration_type       = "AWS_PROXY"
  integration_uri        = var.api_query_invoke_arn
  integration_method     = "POST"
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "query" {
  api_id    = aws_apigatewayv2_api.dashboard.id
  route_key = "GET /query"
  target    = "integrations/${aws_apigatewayv2_integration.dashboard_lambda.id}"
}

resource "aws_apigatewayv2_route" "refresh" {
  api_id    = aws_apigatewayv2_api.dashboard.id
  route_key = "POST /refresh"
  target    = "integrations/${aws_apigatewayv2_integration.dashboard_lambda.id}"
}

resource "aws_apigatewayv2_stage" "prod" {
  api_id      = aws_apigatewayv2_api.dashboard.id
  name        = "prod"
  auto_deploy = true
  tags        = merge(var.common_tags, { Name = "${var.name_prefix}-dashboard-api-prod" })
}

# Lambda permission — owned here (not in lambdas module) to break circular dep
resource "aws_lambda_permission" "allow_apigw_api_query" {
  statement_id  = "AllowAPIGatewayInvokeApiQueryLambda"
  action        = "lambda:InvokeFunction"
  function_name = var.api_query_function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.dashboard.execution_arn}/*/*"
}

# --- S3 Static Website Hosting ---

resource "aws_s3_bucket" "dashboard" {
  bucket        = "${var.name_prefix}-dashboard"
  force_destroy = var.force_destroy
  tags          = merge(var.common_tags, { Name = "${var.name_prefix}-dashboard" })
}

resource "aws_s3_bucket_website_configuration" "dashboard" {
  bucket = aws_s3_bucket.dashboard.id

  index_document { suffix = "index.html" }
  error_document { key = "index.html" }
}

resource "aws_s3_bucket_public_access_block" "dashboard" {
  bucket                  = aws_s3_bucket.dashboard.id
  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

resource "aws_s3_bucket_server_side_encryption_configuration" "dashboard" {
  bucket = aws_s3_bucket.dashboard.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_policy" "dashboard_public_read" {
  bucket = aws_s3_bucket.dashboard.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "PublicReadGetObject"
      Effect    = "Allow"
      Principal = "*"
      Action    = "s3:GetObject"
      Resource  = "${aws_s3_bucket.dashboard.arn}/*"
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.dashboard]
}

# config.js — injected with the real API endpoint at apply time
resource "aws_s3_object" "config_js" {
  bucket       = aws_s3_bucket.dashboard.id
  key          = "config.js"
  content      = "window.API_BASE = \"${aws_apigatewayv2_api.dashboard.api_endpoint}/${aws_apigatewayv2_stage.prod.name}\";\n"
  content_type = "application/javascript"
  tags         = merge(var.common_tags, { Name = "dashboard-config.js" })
  depends_on   = [aws_s3_bucket_public_access_block.dashboard]
}

resource "aws_s3_object" "dashboard_index" {
  bucket       = aws_s3_bucket.dashboard.id
  key          = "index.html"
  source       = var.dashboard_html_path
  etag         = filemd5(var.dashboard_html_path)
  content_type = "text/html"
  tags         = merge(var.common_tags, { Name = "dashboard-index.html" })
  depends_on   = [aws_s3_bucket_public_access_block.dashboard]
}
