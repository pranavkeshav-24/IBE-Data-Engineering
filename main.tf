terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.50"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.5"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ------------------------------------------------------------------------------
# Foundation — suffix, identity, default VPC
# ------------------------------------------------------------------------------

resource "random_id" "suffix" {
  byte_length = 3
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# ------------------------------------------------------------------------------
# Locals — shared name prefix, tags, config, pre-computed ARNs
# ------------------------------------------------------------------------------

locals {
  name_prefix = "${var.project_name}-${random_id.suffix.hex}"

  common_tags = merge(var.tags, {
    ManagedBy = "Terraform"
    Project   = var.project_name
  })

  client_configs = {
    alpha = {
      client_id         = "alpha"
      prefix            = "raw/alpha/"
      required_patterns = ["orders", "codes"]
      optional_patterns = []
      strategy          = "upsert"
      lock_ttl_seconds  = 900
      required_columns = {
        orders = ["order_id", "code", "order_date", "subtotal_price"]
        codes  = ["Promo Code"]
      }
      pipeline = [
        { step = "validate_inputs" },
        { step = "special_transform", name = "alpha" },
        { step = "write_outputs" }
      ]
    }
    beta = {
      client_id         = "beta"
      prefix            = "raw/beta/"
      required_patterns = ["sales", "shows_and_codes"]
      optional_patterns = ["salesforce"]
      strategy          = "upsert"
      lock_ttl_seconds  = 900
      required_columns = {
        sales           = ["date", "Show", "Promo Code", "Sales", "Orders", "New", "Lapsed", "Active"]
        shows_and_codes = ["OMAHA SHOWNAME", "CODE"]
      }
      pipeline = [
        { step = "validate_inputs" },
        { step = "special_transform", name = "beta" },
        { step = "write_outputs" }
      ]
    }
    gamma = {
      client_id         = "gamma"
      prefix            = "raw/gamma/"
      required_patterns = ["sales"]
      optional_patterns = ["salesforce"]
      strategy          = "upsert"
      lock_ttl_seconds  = 900
      required_columns = {
        sales = ["id", "sale_date", "value_eur", "item_id", "units", "code"]
      }
      pipeline = [
        { step = "validate_inputs" },
        { step = "special_transform", name = "gamma" },
        { step = "write_outputs" }
      ]
    }
    adscribe = {
      client_id         = "adscribe"
      prefix            = "raw/adscribe/"
      required_patterns = ["adscribe"]
      optional_patterns = []
      strategy          = "delete_insert"
      lock_ttl_seconds  = 900
      required_columns = {
        adscribe = []
      }
      pipeline = [
        { step = "validate_inputs" },
        { step = "special_transform", name = "adscribe" },
        { step = "write_outputs" }
      ]
    }
  }

  client_config_files = {
    for client_id, cfg in local.client_configs :
    "clients/${client_id}.json" => jsonencode(cfg)
  }

  client_config_index = jsonencode({
    clients = {
      for client_id, cfg in local.client_configs : client_id => {
        prefix            = cfg.prefix
        required_patterns = cfg.required_patterns
        optional_patterns = cfg.optional_patterns
        strategy          = cfg.strategy
        config_key        = "clients/${client_id}.json"
      }
    }
  })

  # Pre-computed ARNs to break circular IAM dependencies
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name

  lambda_dedup_check_arn     = "arn:aws:lambda:${local.region}:${local.account_id}:function:${local.name_prefix}-dedup-check"
  lambda_readiness_check_arn = "arn:aws:lambda:${local.region}:${local.account_id}:function:${local.name_prefix}-readiness-check"
  lambda_redshift_loader_arn = "arn:aws:lambda:${local.region}:${local.account_id}:function:${local.name_prefix}-redshift-loader"
  lambda_mark_success_arn    = "arn:aws:lambda:${local.region}:${local.account_id}:function:${local.name_prefix}-mark-success"

  glue_job_name = "${local.name_prefix}-pipeline-job"
  glue_job_arn  = "arn:aws:glue:${local.region}:${local.account_id}:job/${local.glue_job_name}"

  state_machine_name = "${local.name_prefix}-pipeline"
  state_machine_arn  = "arn:aws:states:${local.region}:${local.account_id}:stateMachine:${local.state_machine_name}"

  # When an existing workgroup is provided, reuse it; otherwise provision one.
  create_redshift_serverless = trimspace(var.existing_redshift_workgroup_name) == ""
  redshift_workgroup_name    = local.create_redshift_serverless ? replace("${local.name_prefix}wg", "-", "") : trimspace(var.existing_redshift_workgroup_name)
}

# ------------------------------------------------------------------------------
# Module: S3 Buckets
# ------------------------------------------------------------------------------

module "s3" {
  source = "./modules/s3"

  name_prefix   = local.name_prefix
  common_tags   = local.common_tags
  force_destroy = var.force_destroy_buckets
  client_configs = merge(
    local.client_config_files,
    { "client_config.json" = local.client_config_index }
  )
}

# ------------------------------------------------------------------------------
# Module: DynamoDB + SNS + Secrets Manager
# ------------------------------------------------------------------------------

module "dynamodb_sns" {
  source = "./modules/dynamodb_sns"

  name_prefix                   = local.name_prefix
  common_tags                   = local.common_tags
  alert_email_subscriptions     = var.alert_email_subscriptions
  existing_redshift_secret_name = var.existing_redshift_secret_name
  redshift_admin_username       = var.redshift_admin_username
  redshift_admin_password       = var.redshift_admin_password
  redshift_database             = var.redshift_database
}

# ------------------------------------------------------------------------------
# Module: IAM Roles & Policies
# Uses pre-computed ARNs to avoid circular dependencies
# ------------------------------------------------------------------------------

module "iam" {
  source = "./modules/iam"

  name_prefix = local.name_prefix
  common_tags = local.common_tags

  # S3 ARNs
  raw_bucket_arn        = module.s3.raw_bucket_arn
  config_bucket_arn     = module.s3.config_bucket_arn
  processed_bucket_arn  = module.s3.processed_bucket_arn
  quarantine_bucket_arn = module.s3.quarantine_bucket_arn

  # Data services
  dynamodb_table_arn               = module.dynamodb_sns.dedup_table_arn
  sns_topic_arn                    = module.dynamodb_sns.sns_topic_arn
  secretsmanager_secret_arn        = module.dynamodb_sns.secretsmanager_secret_arn
  secretsmanager_secret_kms_key_id = module.dynamodb_sns.secretsmanager_secret_kms_key_id

  # Pre-computed ARNs
  state_machine_arn          = local.state_machine_arn
  glue_job_arn               = local.glue_job_arn
  lambda_dedup_check_arn     = local.lambda_dedup_check_arn
  lambda_readiness_check_arn = local.lambda_readiness_check_arn
  lambda_redshift_loader_arn = local.lambda_redshift_loader_arn
  lambda_mark_success_arn    = local.lambda_mark_success_arn
}

# ------------------------------------------------------------------------------
# Module: Glue ETL Job + Crawler + Catalog
# ------------------------------------------------------------------------------

module "glue" {
  source = "./modules/glue"

  name_prefix          = local.name_prefix
  common_tags          = local.common_tags
  glue_role_arn        = module.iam.glue_role_arn
  config_bucket_id     = module.s3.config_bucket_id
  raw_bucket_id        = module.s3.raw_bucket_id
  processed_bucket_id  = module.s3.processed_bucket_id
  quarantine_bucket_id = module.s3.quarantine_bucket_id
  glue_script_path     = "${path.module}/glue/scripts/etl_job.py"
}

# ------------------------------------------------------------------------------
# Module: Redshift Serverless (default VPC subnets)
# Skipped when an existing workgroup is provided.
# ------------------------------------------------------------------------------

module "redshift" {
  count  = local.create_redshift_serverless ? 1 : 0
  source = "./modules/redshift"

  name_prefix             = local.name_prefix
  common_tags             = local.common_tags
  redshift_admin_username = var.redshift_admin_username
  redshift_admin_password = var.redshift_admin_password
  redshift_database       = var.redshift_database
  redshift_s3_role_arn    = module.iam.redshift_s3_role_arn
  subnet_ids              = data.aws_subnets.default.ids
}

# ------------------------------------------------------------------------------
# Module: Step Functions State Machine
# ------------------------------------------------------------------------------

module "step_functions" {
  source = "./modules/step_functions"

  name_prefix             = local.name_prefix
  common_tags             = local.common_tags
  step_functions_role_arn = module.iam.step_functions_role_arn
  sns_topic_arn           = module.dynamodb_sns.sns_topic_arn

  # Lambda ARNs (pre-computed — actual functions created in module.lambdas)
  lambda_dedup_check_arn     = local.lambda_dedup_check_arn
  lambda_readiness_check_arn = local.lambda_readiness_check_arn
  lambda_redshift_loader_arn = local.lambda_redshift_loader_arn
  lambda_mark_success_arn    = local.lambda_mark_success_arn

  glue_job_name        = module.glue.glue_job_name
  processed_bucket_id  = module.s3.processed_bucket_id
  quarantine_bucket_id = module.s3.quarantine_bucket_id
  config_bucket_id     = module.s3.config_bucket_id
}

# ------------------------------------------------------------------------------
# Module: Lambda Functions + S3 Events + EventBridge
# ------------------------------------------------------------------------------

module "lambdas" {
  source = "./modules/lambdas"

  name_prefix         = local.name_prefix
  common_tags         = local.common_tags
  lambdas_source_path = "${path.module}/lambdas"
  build_path          = "${path.module}/build"
  lambda_role_arn     = module.iam.lambda_role_arn

  # S3
  raw_bucket_id       = module.s3.raw_bucket_id
  raw_bucket_arn      = module.s3.raw_bucket_arn
  config_bucket_id    = module.s3.config_bucket_id
  processed_bucket_id = module.s3.processed_bucket_id
  config_prefix       = "clients"
  config_index_key    = "client_config.json"

  # DynamoDB
  dedup_table_name = module.dynamodb_sns.dedup_table_name

  # Secrets
  secretsmanager_secret_arn = module.dynamodb_sns.secretsmanager_secret_arn

  # Redshift (workgroup name computed locally to avoid depending on redshift module)
  redshift_workgroup_name = local.redshift_workgroup_name
  redshift_database       = var.redshift_database
  redshift_schema         = var.redshift_schema
  redshift_skip_ddl       = var.redshift_skip_ddl
  redshift_s3_role_arn    = module.iam.redshift_s3_role_arn

  # Step Functions (pre-computed ARN to avoid circular dep)
  state_machine_arn = local.state_machine_arn

  # Adscribe
  adscribe_api_endpoint    = var.adscribe_api_endpoint
  adscribe_lookback_days   = var.adscribe_lookback_days
  adscribe_cron_expression = var.adscribe_cron_expression

  # API
  api_allowed_cors_origin = var.api_allowed_cors_origin
}

# ------------------------------------------------------------------------------
# Module: API Gateway + S3 Static Dashboard
# Depends on lambdas module for invoke ARN; owns the Lambda permission.
# ------------------------------------------------------------------------------

module "api_dashboard" {
  source = "./modules/api_dashboard"

  name_prefix             = local.name_prefix
  common_tags             = local.common_tags
  force_destroy           = var.force_destroy_buckets
  api_query_invoke_arn    = module.lambdas.api_query_invoke_arn
  api_query_function_name = "${local.name_prefix}-api-query"
  api_allowed_cors_origin = var.api_allowed_cors_origin
  dashboard_html_path     = "${path.module}/dashboard/index.html"
}
