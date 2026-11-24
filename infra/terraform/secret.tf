resource "aws_secretsmanager_secret" "alert_email" {
  name = "world-bank/alert-email"
}
