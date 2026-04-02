# Momentum IBE Data Engineering (Terraform)

This repository provisions the full AWS architecture you requested in **`ap-south-1`**, with required tags on all supported resources:

- `Creator Team = Momentum`
- `Purpose = IBE Data Engineering`

It follows your target design:

- Bronze: EventBridge + Adscribe Lambda + Client CSV bucket + Raw S3
- Orchestration: Dispatcher Lambda + Step Functions + Config S3 + DynamoDB + SNS
- Silver: Glue ETL + Processed S3 + Quarantine S3 + Glue Crawler/Data Catalog
- Gold: Redshift Serverless (`fact_uploads`, `fact_adscribe`)
- Consumption: API Gateway + Lambda + CloudFront + S3 dashboard UI

No explicit CloudWatch log groups/alarms/monitoring resources are created.

## Deploy

```bash
terraform init
terraform plan -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"
```

Use `terraform.tfvars.example` as a starter.

## Important Notes

- Redshift Serverless is not free-tier, but this stack uses the **minimum practical capacity** (`base_capacity = 8`) to control cost.
- Most other services are configured with pay-per-use or serverless defaults.
- If you already have a Redshift Serverless workgroup, set `existing_redshift_workgroup_name` to reuse it. Terraform will skip creating Redshift namespace/workgroup and connect Lambda/Data API to your assigned DB via `redshift_database`.
- If you already created a Redshift credentials secret in Secrets Manager, set `existing_redshift_secret_name` (for example `momentum-ibe-secrets`). Terraform will reuse that secret ARN instead of creating a new one.
- The reused secret should include at least: `username`, `password`, and `dbname` (extra keys like `host`, `port`, `namespaceName`, `engine` are fine).
- Glue is configured with low worker count for cost control.
- For data load correctness, client uploads should preserve folder conventions:
  - `raw/alpha/<batch>/...orders...csv` + `...codes...csv`
  - `raw/beta/<batch>/...sales...csv` + `...shows_and_codes...csv` (+ optional salesforce file)
  - `raw/gamma/<batch>/...sales...csv` (+ optional salesforce file)
  - Adscribe Lambda writes to `raw/adscribe/<date>/...csv`

## Pipeline Behavior

- Idempotency:
  - Deterministic Step Functions execution names (duplicate S3 event safe).
  - DynamoDB tracks per-file-version and latest pointers (`version#...`, `latest#...`) for pair-aware dedup.
- Multi-file readiness:
  - Required and optional file pattern discovery per client config (`clients/<client>.json`).
  - Client run-lock in DynamoDB prevents concurrent duplicate processing for the same client.
  - Header/schema validation before Glue starts.
- ETL:
  - Glue executes ordered pipeline steps from client config.
  - One shared job with named special transforms (`alpha`, `beta`, `gamma`, `adscribe`).
  - Glue execution class is `FLEX` for cost optimization.
- Adscribe ingest:
  - Presigned CSV is streamed directly to S3 (no temp file).
  - Date window is clamped to allowed API bounds and retried with a fresh URL on download failure.
- Load:
  - Client uploads use `upsert`; Adscribe uses `delete_insert`.
  - Redshift loader skips COPY safely when a run has zero load rows (all quarantined or empty input).
- Failure handling: retries at each stage; max failure sends SNS alert.

## Outputs

After apply, Terraform outputs:

- Bucket names (`client_csvs`, `raw`, `config`, `processed`, `quarantine`)
- Step Functions ARN
- Redshift namespace/workgroup
- API endpoint
- CloudFront dashboard URL
- SNS failure topic ARN
