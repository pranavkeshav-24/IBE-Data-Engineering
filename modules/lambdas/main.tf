# ------------------------------------------------------------------------------
# Lambda Functions — 7 functions + EventBridge + S3 event notification
# Note: API Gateway Lambda permission is in modules/api_dashboard to avoid
#       circular dependency (api_dashboard needs lambdas ARN; lambdas would
#       need api_dashboard execution ARN).
# ------------------------------------------------------------------------------

data "archive_file" "adscribe_fetcher" {
  type        = "zip"
  source_dir  = "${var.lambdas_source_path}/adscribe_fetcher"
  output_path = "${var.build_path}/adscribe_fetcher.zip"
}

data "archive_file" "dispatcher" {
  type        = "zip"
  source_dir  = "${var.lambdas_source_path}/dispatcher"
  output_path = "${var.build_path}/dispatcher.zip"
}

data "archive_file" "dedup_check" {
  type        = "zip"
  source_dir  = "${var.lambdas_source_path}/dedup_check"
  output_path = "${var.build_path}/dedup_check.zip"
}

data "archive_file" "readiness_check" {
  type        = "zip"
  source_dir  = "${var.lambdas_source_path}/readiness_check"
  output_path = "${var.build_path}/readiness_check.zip"
}

data "archive_file" "mark_success" {
  type        = "zip"
  source_dir  = "${var.lambdas_source_path}/mark_success"
  output_path = "${var.build_path}/mark_success.zip"
}

data "archive_file" "redshift_loader" {
  type        = "zip"
  source_dir  = "${var.lambdas_source_path}/redshift_loader"
  output_path = "${var.build_path}/redshift_loader.zip"
}

data "archive_file" "api_query" {
  type        = "zip"
  source_dir  = "${var.lambdas_source_path}/api_query"
  output_path = "${var.build_path}/api_query.zip"
}

# --- Adscribe Fetcher ---
resource "aws_lambda_function" "adscribe_fetcher" {
  function_name    = "${var.name_prefix}-adscribe-fetcher"
  role             = var.lambda_role_arn
  runtime          = "python3.12"
  handler          = "app.lambda_handler"
  filename         = data.archive_file.adscribe_fetcher.output_path
  source_code_hash = data.archive_file.adscribe_fetcher.output_base64sha256
  timeout          = 900
  memory_size      = 1024

  environment {
    variables = {
      ADSCRIBE_API_ENDPOINT = var.adscribe_api_endpoint
      RAW_BUCKET            = var.raw_bucket_id
      RAW_PREFIX            = "raw/adscribe"
      LOOKBACK_DAYS         = tostring(var.adscribe_lookback_days)
    }
  }

  tags = merge(var.common_tags, { Name = "${var.name_prefix}-adscribe-fetcher" })
}

# --- Dispatcher ---
resource "aws_lambda_function" "dispatcher" {
  function_name    = "${var.name_prefix}-dispatcher"
  role             = var.lambda_role_arn
  runtime          = "python3.12"
  handler          = "app.lambda_handler"
  filename         = data.archive_file.dispatcher.output_path
  source_code_hash = data.archive_file.dispatcher.output_base64sha256
  timeout          = 120
  memory_size      = 256

  environment {
    variables = {
      STATE_MACHINE_ARN = var.state_machine_arn
    }
  }

  tags = merge(var.common_tags, { Name = "${var.name_prefix}-dispatcher" })
}

# --- Dedup Check ---
resource "aws_lambda_function" "dedup_check" {
  function_name    = "${var.name_prefix}-dedup-check"
  role             = var.lambda_role_arn
  runtime          = "python3.12"
  handler          = "app.lambda_handler"
  filename         = data.archive_file.dedup_check.output_path
  source_code_hash = data.archive_file.dedup_check.output_base64sha256
  timeout          = 30
  memory_size      = 256

  environment {
    variables = {
      DEDUP_TABLE_NAME = var.dedup_table_name
      CONFIG_BUCKET    = var.config_bucket_id
      CONFIG_PREFIX    = var.config_prefix
    }
  }

  tags = merge(var.common_tags, { Name = "${var.name_prefix}-dedup-check" })
}

# --- Readiness Check ---
resource "aws_lambda_function" "readiness_check" {
  function_name    = "${var.name_prefix}-readiness-check"
  role             = var.lambda_role_arn
  runtime          = "python3.12"
  handler          = "app.lambda_handler"
  filename         = data.archive_file.readiness_check.output_path
  source_code_hash = data.archive_file.readiness_check.output_base64sha256
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      CONFIG_BUCKET    = var.config_bucket_id
      CONFIG_PREFIX    = var.config_prefix
      CONFIG_INDEX_KEY = var.config_index_key
      DEDUP_TABLE_NAME = var.dedup_table_name
    }
  }

  tags = merge(var.common_tags, { Name = "${var.name_prefix}-readiness-check" })
}

# --- Mark Success ---
resource "aws_lambda_function" "mark_success" {
  function_name    = "${var.name_prefix}-mark-success"
  role             = var.lambda_role_arn
  runtime          = "python3.12"
  handler          = "app.lambda_handler"
  filename         = data.archive_file.mark_success.output_path
  source_code_hash = data.archive_file.mark_success.output_base64sha256
  timeout          = 30
  memory_size      = 256

  environment {
    variables = {
      DEDUP_TABLE_NAME = var.dedup_table_name
    }
  }

  tags = merge(var.common_tags, { Name = "${var.name_prefix}-mark-success" })
}

# --- Redshift Loader ---
resource "aws_lambda_function" "redshift_loader" {
  function_name    = "${var.name_prefix}-redshift-loader"
  role             = var.lambda_role_arn
  runtime          = "python3.12"
  handler          = "app.lambda_handler"
  filename         = data.archive_file.redshift_loader.output_path
  source_code_hash = data.archive_file.redshift_loader.output_base64sha256
  timeout          = 900
  memory_size      = 512

  environment {
    variables = {
      REDSHIFT_WORKGROUP     = var.redshift_workgroup_name
      REDSHIFT_DATABASE      = var.redshift_database
      REDSHIFT_SECRET_ARN    = var.secretsmanager_secret_arn
      PROCESSED_BUCKET       = var.processed_bucket_id
      REDSHIFT_COPY_ROLE_ARN = var.redshift_s3_role_arn
      REDSHIFT_SCHEMA        = var.redshift_schema
      REDSHIFT_SKIP_DDL      = tostring(var.redshift_skip_ddl)
    }
  }

  tags = merge(var.common_tags, { Name = "${var.name_prefix}-redshift-loader" })
}

# --- API Query ---
resource "aws_lambda_function" "api_query" {
  function_name    = "${var.name_prefix}-api-query"
  role             = var.lambda_role_arn
  runtime          = "python3.12"
  handler          = "app.lambda_handler"
  filename         = data.archive_file.api_query.output_path
  source_code_hash = data.archive_file.api_query.output_base64sha256
  timeout          = 120
  memory_size      = 512

  environment {
    variables = {
      REDSHIFT_WORKGROUP  = var.redshift_workgroup_name
      REDSHIFT_DATABASE   = var.redshift_database
      REDSHIFT_SECRET_ARN = var.secretsmanager_secret_arn
      REDSHIFT_SCHEMA     = var.redshift_schema
      STATE_MACHINE_ARN   = var.state_machine_arn
      ALLOWED_ORIGIN      = var.api_allowed_cors_origin
    }
  }

  tags = merge(var.common_tags, { Name = "${var.name_prefix}-api-query" })
}

# --- S3 → Dispatcher event notification ---
resource "aws_lambda_permission" "allow_s3_dispatcher" {
  statement_id  = "AllowRawBucketInvokeDispatcher"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.dispatcher.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = var.raw_bucket_arn
}

resource "aws_s3_bucket_notification" "raw_events" {
  bucket = var.raw_bucket_id

  lambda_function {
    lambda_function_arn = aws_lambda_function.dispatcher.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.allow_s3_dispatcher]
}

# --- EventBridge daily cron → Adscribe Fetcher ---
resource "aws_cloudwatch_event_rule" "adscribe_daily" {
  name                = "${var.name_prefix}-adscribe-daily"
  schedule_expression = var.adscribe_cron_expression
  tags                = merge(var.common_tags, { Name = "${var.name_prefix}-adscribe-daily" })
}

resource "aws_cloudwatch_event_target" "adscribe_lambda" {
  rule      = aws_cloudwatch_event_rule.adscribe_daily.name
  target_id = "adscribe-fetcher"
  arn       = aws_lambda_function.adscribe_fetcher.arn
}

resource "aws_lambda_permission" "allow_eventbridge_adscribe" {
  statement_id  = "AllowEventBridgeInvokeAdscribeFetcher"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.adscribe_fetcher.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.adscribe_daily.arn
}
