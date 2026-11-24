#!/bin/sh
set -eu

unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE

if [ -n "${AWS_ACCESS_KEY_ID:-}${AWS_SECRET_ACCESS_KEY:-}${AWS_SESSION_TOKEN:-}${AWS_PROFILE:-}" ]; then
  echo "error: real AWS credential variables are still set" >&2
  exit 1
fi

export AWS_ENDPOINT_URL=https://127.0.0.1:9297
export AWS_SHARED_CREDENTIALS_FILE="${HOME}/.aws/emulator/9297/emulator_credentials"
export AWS_CONFIG_FILE="${HOME}/.aws/emulator/9297/config"
export AWS_CA_BUNDLE="${HOME}/.aws/emulator/9297/ca.pem"
export AWS_EC2_METADATA_DISABLED=true
export AWS_DISABLE_HOST_PREFIX_INJECTION=true
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
export AWS_PAGER=""

if [ ! -f "$AWS_SHARED_CREDENTIALS_FILE" ]; then
  echo "error: emulator credentials are not on this machine" >&2
  exit 1
fi
if [ ! -f "$AWS_CA_BUNDLE" ]; then
  echo "error: emulator certificate is not on this machine" >&2
  exit 1
fi

# Terraform lives in ~/.local/bin. This lasts only for the exec'd command.
export PATH="${HOME}/.local/bin:${PATH}"

exec "$@"
