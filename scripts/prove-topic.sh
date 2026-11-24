#!/bin/sh
set -eu

cd "$(dirname "$0")/.."

topic=$(terraform -chdir=infra/terraform output -raw topic_arn)

message_id=$(aws sns publish \
  --topic-arn "$topic" \
  --message "platform probe" \
  --query MessageId \
  --output text)
if [ -z "$message_id" ] || [ "$message_id" = "None" ]; then
  echo "error: publish returned no message id" >&2
  exit 1
fi
echo "publish message id: ${message_id}"

if ! aws secretsmanager describe-secret --secret-id world-bank/alert-email >/dev/null; then
  echo "error: secret does not exist" >&2
  exit 1
fi

secret_err=$(mktemp)
if aws secretsmanager get-secret-value --secret-id world-bank/alert-email >/dev/null 2>"$secret_err"; then
  rm -f "$secret_err"
  echo "error: secret has a value" >&2
  exit 1
fi
if ! grep -q 'ResourceNotFoundException' "$secret_err"; then
  echo "error: get-secret-value failed" >&2
  cat "$secret_err" >&2
  rm -f "$secret_err"
  exit 1
fi
rm -f "$secret_err"

for bucket in lakehouse-raw lakehouse-stage lakehouse-curated; do
  if ! aws s3api head-bucket --bucket "$bucket" >/dev/null; then
    echo "error: bucket ${bucket} does not exist" >&2
    exit 1
  fi
done

logs=$(aws logs describe-log-groups --log-group-name-prefix /world-bank/ --output json)
printf '%s' "$logs" | python3 -c '
import json, sys
wanted = {
    "/world-bank/airflow",
    "/world-bank/lakehouse-ingest-world-bank",
    "/world-bank/lakehouse-stage-world-bank",
    "/world-bank/lakehouse-consumer-postgres",
    "/world-bank/dbt-postgres",
}
doc = json.load(sys.stdin)
found = {group["logGroupName"] for group in doc.get("logGroups", [])}
missing = sorted(wanted - found)
if missing:
    sys.stderr.write("error: log groups missing: %s\n" % ", ".join(missing))
    sys.exit(1)
'

if [ ! -f secrets/alert_email.enc ]; then
  subs=$(aws sns list-subscriptions-by-topic --topic-arn "$topic" --output json)
  printf '%s' "$subs" | python3 -c '
import json, sys
doc = json.load(sys.stdin)
subs = doc.get("Subscriptions") or []
if subs:
    sys.stderr.write("error: topic has a subscription\n")
    sys.exit(1)
'
fi
