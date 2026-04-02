# ------------------------------------------------------------------------------
# S3 Buckets — Raw, Processed, Quarantine, Config, ClientCSVs
# Replication: client_csvs → raw
# ------------------------------------------------------------------------------

resource "aws_s3_bucket" "client_csvs" {
  bucket        = "${var.name_prefix}-client-csvs"
  force_destroy = var.force_destroy
  tags          = merge(var.common_tags, { Name = "${var.name_prefix}-client-csvs" })
}

resource "aws_s3_bucket" "raw" {
  bucket        = "${var.name_prefix}-raw"
  force_destroy = var.force_destroy
  tags          = merge(var.common_tags, { Name = "${var.name_prefix}-raw" })
}

resource "aws_s3_bucket" "config" {
  bucket        = "${var.name_prefix}-config"
  force_destroy = var.force_destroy
  tags          = merge(var.common_tags, { Name = "${var.name_prefix}-config" })
}

resource "aws_s3_bucket" "processed" {
  bucket        = "${var.name_prefix}-processed"
  force_destroy = var.force_destroy
  tags          = merge(var.common_tags, { Name = "${var.name_prefix}-processed" })
}

resource "aws_s3_bucket" "quarantine" {
  bucket        = "${var.name_prefix}-quarantine"
  force_destroy = var.force_destroy
  tags          = merge(var.common_tags, { Name = "${var.name_prefix}-quarantine" })
}

# --- Versioning (all buckets) ---

resource "aws_s3_bucket_versioning" "versioning" {
  for_each = {
    client_csvs = aws_s3_bucket.client_csvs.id
    raw         = aws_s3_bucket.raw.id
    config      = aws_s3_bucket.config.id
    processed   = aws_s3_bucket.processed.id
    quarantine  = aws_s3_bucket.quarantine.id
  }

  bucket = each.value
  versioning_configuration {
    status = "Enabled"
  }
}

# --- Server-side encryption (AES256, all buckets) ---

resource "aws_s3_bucket_server_side_encryption_configuration" "sse" {
  for_each = {
    client_csvs = aws_s3_bucket.client_csvs.id
    raw         = aws_s3_bucket.raw.id
    config      = aws_s3_bucket.config.id
    processed   = aws_s3_bucket.processed.id
    quarantine  = aws_s3_bucket.quarantine.id
  }

  bucket = each.value
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# --- Block public access (all pipeline buckets) ---

resource "aws_s3_bucket_public_access_block" "public_block" {
  for_each = {
    client_csvs = aws_s3_bucket.client_csvs.id
    raw         = aws_s3_bucket.raw.id
    config      = aws_s3_bucket.config.id
    processed   = aws_s3_bucket.processed.id
    quarantine  = aws_s3_bucket.quarantine.id
  }

  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- Upload per-client config JSON files to config bucket ---

resource "aws_s3_object" "client_config" {
  for_each = var.client_configs

  bucket       = aws_s3_bucket.config.id
  key          = each.key
  content_type = "application/json"
  content      = each.value
  tags         = merge(var.common_tags, { Name = each.key })
}

# --- S3 Replication: client_csvs → raw ---

data "aws_iam_policy_document" "s3_replication_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "s3_replication" {
  name               = "${var.name_prefix}-s3-replication-role"
  assume_role_policy = data.aws_iam_policy_document.s3_replication_assume.json
  tags               = merge(var.common_tags, { Name = "${var.name_prefix}-s3-replication-role" })
}

data "aws_iam_policy_document" "s3_replication_policy" {
  statement {
    actions   = ["s3:GetReplicationConfiguration", "s3:ListBucket"]
    resources = [aws_s3_bucket.client_csvs.arn]
  }

  statement {
    actions = [
      "s3:GetObjectVersionForReplication",
      "s3:GetObjectVersionAcl",
      "s3:GetObjectVersionTagging"
    ]
    resources = ["${aws_s3_bucket.client_csvs.arn}/*"]
  }

  statement {
    actions = [
      "s3:ReplicateObject",
      "s3:ReplicateDelete",
      "s3:ReplicateTags"
    ]
    resources = ["${aws_s3_bucket.raw.arn}/*"]
  }
}

resource "aws_iam_role_policy" "s3_replication_policy" {
  name   = "${var.name_prefix}-s3-replication-policy"
  role   = aws_iam_role.s3_replication.id
  policy = data.aws_iam_policy_document.s3_replication_policy.json
}

resource "aws_s3_bucket_replication_configuration" "client_to_raw" {
  role   = aws_iam_role.s3_replication.arn
  bucket = aws_s3_bucket.client_csvs.id

  rule {
    id     = "replicate-to-raw"
    status = "Enabled"

    filter {}

    delete_marker_replication {
      status = "Disabled"
    }

    destination {
      bucket        = aws_s3_bucket.raw.arn
      storage_class = "STANDARD"
    }
  }

  depends_on = [
    aws_s3_bucket_versioning.versioning,
    aws_iam_role_policy.s3_replication_policy
  ]
}
