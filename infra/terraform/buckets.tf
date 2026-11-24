locals {
  buckets = toset([
    "lakehouse-raw",
    "lakehouse-stage",
    "lakehouse-curated",
  ])
}

resource "aws_s3_bucket" "lake" {
  for_each      = local.buckets
  bucket        = each.value
  force_destroy = true
}
