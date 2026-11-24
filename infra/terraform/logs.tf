locals {
  log_groups = toset([
    "/world-bank/airflow",
    "/world-bank/lakehouse-ingest-world-bank",
    "/world-bank/lakehouse-stage-world-bank",
    "/world-bank/lakehouse-consumer-postgres",
    "/world-bank/dbt-postgres",
  ])
}

resource "aws_cloudwatch_log_group" "groups" {
  for_each = local.log_groups
  name     = each.value
}
