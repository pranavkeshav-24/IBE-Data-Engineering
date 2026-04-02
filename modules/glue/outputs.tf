output "glue_job_name" {
  value = aws_glue_job.pipeline_job.name
}

output "glue_job_arn" {
  value = aws_glue_job.pipeline_job.arn
}

output "catalog_database_name" {
  value = aws_glue_catalog_database.silver.name
}
