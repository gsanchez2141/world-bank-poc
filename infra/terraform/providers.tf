provider "aws" {
  region                      = "us-east-1"
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3             = "https://127.0.0.1:9297"
    sns            = "https://127.0.0.1:9297"
    secretsmanager = "https://127.0.0.1:9297"
    logs           = "https://127.0.0.1:9297"
    sts            = "https://127.0.0.1:9297"
    iam            = "https://127.0.0.1:9297"
  }
}
