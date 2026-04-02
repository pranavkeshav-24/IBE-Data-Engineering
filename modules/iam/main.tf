# ------------------------------------------------------------------------------
# IAM Roles & Policies — Lambda, Step Functions, Glue, Redshift
# Uses constructed ARNs for resources not yet created (breaks circular deps)
# ------------------------------------------------------------------------------

# ======================== Lambda Role ========================

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_role" {
  name               = "${var.name_prefix}-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
  tags               = merge(var.common_tags, { Name = "${var.name_prefix}-lambda-role" })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

locals {
  secret_kms_key_is_arn = can(regex("^arn:aws[a-z-]*:kms:", var.secretsmanager_secret_kms_key_id))
}

data "aws_iam_policy_document" "lambda_inline" {
  statement {
    sid = "S3ReadWritePipelineBuckets"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket",
      "s3:DeleteObject"
    ]
    resources = flatten([
      for arn in [var.raw_bucket_arn, var.config_bucket_arn, var.processed_bucket_arn, var.quarantine_bucket_arn] :
      [arn, "${arn}/*"]
    ])
  }

  statement {
    sid = "StartStateMachineExecutions"
    actions = [
      "states:StartExecution",
      "states:DescribeExecution",
      "states:DescribeStateMachine"
    ]
    resources = [var.state_machine_arn]
  }

  statement {
    sid = "DynamoDBDedup"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem"
    ]
    resources = [var.dynamodb_table_arn]
  }

  statement {
    sid = "RedshiftDataAPI"
    actions = [
      "redshift-data:ExecuteStatement",
      "redshift-data:BatchExecuteStatement",
      "redshift-data:DescribeStatement",
      "redshift-data:GetStatementResult",
      "redshift-data:CancelStatement"
    ]
    resources = ["*"]
  }

  statement {
    sid       = "GetRedshiftCredentials"
    actions   = ["redshift-serverless:GetCredentials"]
    resources = ["*"]
  }

  statement {
    sid       = "ReadRedshiftSecret"
    actions   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
    resources = [var.secretsmanager_secret_arn]
  }

  dynamic "statement" {
    for_each = local.secret_kms_key_is_arn ? [1] : []
    content {
      sid       = "DecryptRedshiftSecretWithKms"
      actions   = ["kms:Decrypt", "kms:DescribeKey"]
      resources = [var.secretsmanager_secret_kms_key_id]
    }
  }

  statement {
    sid       = "PublishAlerts"
    actions   = ["sns:Publish"]
    resources = [var.sns_topic_arn]
  }
}

resource "aws_iam_role_policy" "lambda_inline" {
  name   = "${var.name_prefix}-lambda-inline"
  role   = aws_iam_role.lambda_role.id
  policy = data.aws_iam_policy_document.lambda_inline.json
}

# ======================== Step Functions Role ========================

data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "step_functions_role" {
  name               = "${var.name_prefix}-stepfunctions-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
  tags               = merge(var.common_tags, { Name = "${var.name_prefix}-stepfunctions-role" })
}

data "aws_iam_policy_document" "sfn_inline" {
  statement {
    sid     = "InvokeWorkerLambdas"
    actions = ["lambda:InvokeFunction"]
    resources = [
      var.lambda_dedup_check_arn,
      var.lambda_readiness_check_arn,
      var.lambda_redshift_loader_arn,
      var.lambda_mark_success_arn
    ]
  }

  statement {
    sid = "RunGlueAndReadStatus"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun"
    ]
    resources = [var.glue_job_arn]
  }

  statement {
    sid       = "PublishFailureAlerts"
    actions   = ["sns:Publish"]
    resources = [var.sns_topic_arn]
  }
}

resource "aws_iam_role_policy" "sfn_inline" {
  name   = "${var.name_prefix}-stepfunctions-inline"
  role   = aws_iam_role.step_functions_role.id
  policy = data.aws_iam_policy_document.sfn_inline.json
}

# ======================== Glue Role ========================

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_role" {
  name               = "${var.name_prefix}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
  tags               = merge(var.common_tags, { Name = "${var.name_prefix}-glue-role" })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_inline" {
  statement {
    sid = "S3PipelineAccess"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket"
    ]
    resources = flatten([
      for arn in [var.raw_bucket_arn, var.config_bucket_arn, var.processed_bucket_arn, var.quarantine_bucket_arn] :
      [arn, "${arn}/*"]
    ])
  }
}

resource "aws_iam_role_policy" "glue_inline" {
  name   = "${var.name_prefix}-glue-inline"
  role   = aws_iam_role.glue_role.id
  policy = data.aws_iam_policy_document.glue_inline.json
}

# ======================== Redshift S3 Role ========================

data "aws_iam_policy_document" "redshift_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift_s3_role" {
  name               = "${var.name_prefix}-redshift-s3-role"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
  tags               = merge(var.common_tags, { Name = "${var.name_prefix}-redshift-s3-role" })
}

data "aws_iam_policy_document" "redshift_s3_inline" {
  statement {
    sid = "ReadProcessedData"
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
    resources = [
      var.processed_bucket_arn,
      "${var.processed_bucket_arn}/*"
    ]
  }
}

resource "aws_iam_role_policy" "redshift_s3_inline" {
  name   = "${var.name_prefix}-redshift-s3-inline"
  role   = aws_iam_role.redshift_s3_role.id
  policy = data.aws_iam_policy_document.redshift_s3_inline.json
}
