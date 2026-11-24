#!/bin/sh
set -eu

CA="${HOME}/.aws/emulator/9297/ca.pem"
CREDS="${HOME}/.aws/emulator/9297/emulator_credentials"

if [ ! -f "$CA" ] || [ ! -f "$CREDS" ]; then
  echo "error: emulator certificate or credentials are not available" >&2
  exit 1
fi

store=/tmp/private-cacerts
cp "${JAVA_HOME}/lib/security/cacerts" "$store"
chmod 600 "$store"
keytool -importcert -noprompt -alias emulator \
  -keystore "$store" \
  -storepass changeit \
  -file "$CA" >/dev/null

access_key=$(awk -F= '/aws_access_key_id/ { gsub(/[[:space:]]/, "", $2); print $2; exit }' "$CREDS")
secret_key=$(awk -F= '/aws_secret_access_key/ { gsub(/[[:space:]]/, "", $2); print $2; exit }' "$CREDS")
if [ -z "$access_key" ] || [ -z "$secret_key" ]; then
  echo "error: emulator credentials file has no keys" >&2
  exit 1
fi

export AWS_ACCESS_KEY_ID="$access_key"
export AWS_SECRET_ACCESS_KEY="$secret_key"
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
export AWS_EC2_METADATA_DISABLED=true
export CATALOG_S3_ACCESS__KEY__ID="$access_key"
export CATALOG_S3_SECRET__ACCESS__KEY="$secret_key"
unset AWS_PROFILE AWS_SESSION_TOKEN || true

cd /usr/lib/iceberg-rest
exec java \
  -Djavax.net.ssl.trustStore="$store" \
  -Djavax.net.ssl.trustStorePassword=changeit \
  -Djavax.net.ssl.trustStoreType=JKS \
  -cp "/usr/lib/iceberg-rest/*" \
  org.apache.iceberg.rest.RESTCatalogServer
