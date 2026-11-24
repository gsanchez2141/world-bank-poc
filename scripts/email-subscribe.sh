#!/bin/sh
set -eu

cd "$(dirname "$0")/.."

cipher=secrets/alert_email.enc
key=secrets/email.key

if [ ! -f "$cipher" ]; then
  echo "subscription skipped"
  exit 0
fi

if [ ! -f "$key" ]; then
  echo "warn: ciphertext is present and the key is not on this machine" >&2
  exit 0
fi

if ! plaintext=$(openssl enc -aes-256-cbc -d -salt -pbkdf2 -pass "file:${key}" -in "$cipher"); then
  echo "error: decrypt failed" >&2
  exit 1
fi

aws secretsmanager put-secret-value \
  --secret-id world-bank/alert-email \
  --secret-string "$plaintext" >/dev/null

topic=$(terraform -chdir=infra/terraform output -raw topic_arn)
aws sns subscribe \
  --topic-arn "$topic" \
  --protocol email \
  --notification-endpoint "$plaintext" >/dev/null

unset plaintext
echo "subscription created"
