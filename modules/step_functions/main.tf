# ------------------------------------------------------------------------------
# Step Functions State Machine — Full configurable ingestion pipeline
# ------------------------------------------------------------------------------

locals {
  pipeline_state_machine = {
    Comment = "Momentum configurable ingestion pipeline"
    StartAt = "InjectContext"
    States = {
      InjectContext = {
        Type = "Pass"
        Parameters = {
          "bucket.$"      = "$.bucket"
          "key.$"         = "$.key"
          "client_id.$"   = "$.client_id"
          "file_hash.$"   = "$.file_hash"
          "object_size.$" = "$.object_size"
          "event_time.$"  = "$.event_time"
          "run_id.$"      = "$$.Execution.Name"
        }
        Next = "CheckDedup"
      }

      CheckDedup = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.lambda_dedup_check_arn
          "Payload.$"  = "$"
        }
        OutputPath = "$.Payload"
        Retry = [{
          ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"]
          IntervalSeconds = 2
          MaxAttempts     = 3
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        Next = "AlreadyProcessed?"
      }

      "AlreadyProcessed?" = {
        Type = "Choice"
        Choices = [{
          Variable      = "$.already_processed"
          BooleanEquals = true
          Next          = "SkipAlreadyProcessed"
        }]
        Default = "CheckReadiness"
      }

      SkipAlreadyProcessed = {
        Type = "Succeed"
      }

      CheckReadiness = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.lambda_readiness_check_arn
          "Payload.$"  = "$"
        }
        OutputPath = "$.Payload"
        Retry = [{
          ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"]
          IntervalSeconds = 2
          MaxAttempts     = 3
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        Next = "FilesReady?"
      }

      "FilesReady?" = {
        Type = "Choice"
        Choices = [{
          Variable      = "$.ready"
          BooleanEquals = true
          Next          = "StartGlueJob"
        }]
        Default = "WaitingForMoreFiles"
      }

      WaitingForMoreFiles = {
        Type = "Succeed"
      }

      StartGlueJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = var.glue_job_name
          Arguments = {
            "--RAW_BUCKET.$"       = "$.bucket"
            "--PROCESSED_BUCKET"   = var.processed_bucket_id
            "--QUARANTINE_BUCKET"  = var.quarantine_bucket_id
            "--CONFIG_BUCKET"      = var.config_bucket_id
            "--CONFIG_KEY.$"       = "$.config_key"
            "--CLIENT_ID.$"        = "$.client_id"
            "--BUCKET.$"           = "$.bucket"
            "--KEY.$"              = "$.key"
            "--FILE_HASH.$"        = "$.file_hash"
            "--RUN_ID.$"           = "$.run_id"
            "--INPUT_FILES_JSON.$" = "States.JsonToString($.input_files)"
          }
        }
        ResultPath = "$.glue_status"
        Retry = [{
          ErrorEquals     = ["States.TaskFailed"]
          IntervalSeconds = 5
          MaxAttempts     = 3
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        Next = "LoadToRedshift"
      }

      LoadToRedshift = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.lambda_redshift_loader_arn
          "Payload.$"  = "$"
        }
        OutputPath = "$.Payload"
        Retry = [{
          ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"]
          IntervalSeconds = 5
          MaxAttempts     = 3
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        Next = "WriteDedupSuccess"
      }

      WriteDedupSuccess = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.lambda_mark_success_arn
          "Payload.$"  = "$"
        }
        OutputPath = "$.Payload"
        Retry = [{
          ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"]
          IntervalSeconds = 2
          MaxAttempts     = 3
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        Next = "PipelineSuccess"
      }

      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = var.sns_topic_arn
          Subject     = "Momentum ETL pipeline failure"
          "Message.$" = "States.JsonToString($)"
        }
        Next = "PipelineFailed"
      }

      PipelineFailed = {
        Type  = "Fail"
        Cause = "Pipeline execution failed after retries."
      }

      PipelineSuccess = {
        Type = "Succeed"
      }
    }
  }
}

resource "aws_sfn_state_machine" "pipeline" {
  name       = "${var.name_prefix}-pipeline"
  role_arn   = var.step_functions_role_arn
  definition = jsonencode(local.pipeline_state_machine)
  type       = "STANDARD"
  tags       = merge(var.common_tags, { Name = "${var.name_prefix}-pipeline" })
}
