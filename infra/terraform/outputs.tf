output "topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "secret_name" {
  value = aws_secretsmanager_secret.alert_email.name
}

output "bucket_names" {
  value = sort([for bucket in aws_s3_bucket.lake : bucket.bucket])
}

output "log_group_names" {
  value = sort([for group in aws_cloudwatch_log_group.groups : group.name])
}
