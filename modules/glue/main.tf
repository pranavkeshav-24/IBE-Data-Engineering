# ------------------------------------------------------------------------------
# Glue Catalog, ETL Job, Crawler, Script Upload
# ------------------------------------------------------------------------------

resource "aws_s3_object" "glue_script" {
  bucket       = var.config_bucket_id
  key          = "scripts/etl_job.py"
  source       = var.glue_script_path
  etag         = filemd5(var.glue_script_path)
  content_type = "text/x-python"
  tags         = merge(var.common_tags, { Name = "etl_job.py" })
}

resource "aws_glue_catalog_database" "silver" {
  name = replace("${var.name_prefix}_silver", "-", "_")
}

resource "aws_glue_job" "pipeline_job" {
  name              = "${var.name_prefix}-pipeline-job"
  role_arn          = var.glue_role_arn
  glue_version      = "4.0"
  execution_class   = "FLEX"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = "s3://${var.config_bucket_id}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "false"
    "--enable-observability-metrics"     = "false"
    "--RAW_BUCKET"                       = var.raw_bucket_id
    "--PROCESSED_BUCKET"                 = var.processed_bucket_id
    "--QUARANTINE_BUCKET"                = var.quarantine_bucket_id
    "--CONFIG_BUCKET"                    = var.config_bucket_id
    "--CONFIG_KEY"                       = "client_config.json"
  }

  execution_property {
    max_concurrent_runs = 5
  }

  tags = merge(var.common_tags, { Name = "${var.name_prefix}-pipeline-job" })

  depends_on = [aws_s3_object.glue_script]
}

resource "aws_glue_crawler" "processed" {
  name          = "${var.name_prefix}-processed-crawler"
  database_name = aws_glue_catalog_database.silver.name
  role          = var.glue_role_arn
  table_prefix  = "silver_"

  s3_target {
    path = "s3://${var.processed_bucket_id}/silver/curated/"
  }

  schedule = "cron(0 4 * * ? *)"

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_NEW_FOLDERS_ONLY"
  }

  tags = merge(var.common_tags, { Name = "${var.name_prefix}-processed-crawler" })
}
