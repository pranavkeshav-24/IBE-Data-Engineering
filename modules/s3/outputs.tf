output "client_csvs_bucket_id" {
  value = aws_s3_bucket.client_csvs.id
}

output "client_csvs_bucket_arn" {
  value = aws_s3_bucket.client_csvs.arn
}

output "raw_bucket_id" {
  value = aws_s3_bucket.raw.id
}

output "raw_bucket_arn" {
  value = aws_s3_bucket.raw.arn
}

output "config_bucket_id" {
  value = aws_s3_bucket.config.id
}

output "config_bucket_arn" {
  value = aws_s3_bucket.config.arn
}

output "processed_bucket_id" {
  value = aws_s3_bucket.processed.id
}

output "processed_bucket_arn" {
  value = aws_s3_bucket.processed.arn
}

output "quarantine_bucket_id" {
  value = aws_s3_bucket.quarantine.id
}

output "quarantine_bucket_arn" {
  value = aws_s3_bucket.quarantine.arn
}
