#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="/opt/airflow/project/.env"

if [[ -f "${ENV_FILE}" ]]; then
  while IFS='=' read -r key value; do
    key="${key%$'\r'}"
    value="${value%$'\r'}"
    key="${key#export }"

    if [[ -z "${key}" || "${key}" == \#* ]]; then
      continue
    fi

    if [[ "${value}" == \"*\" && "${value}" == *\" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "${value}" == \'*\' && "${value}" == *\' ]]; then
      value="${value:1:${#value}-2}"
    fi

    if [[ "${key}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
      export "${key}=${value}"
    fi
  done < "${ENV_FILE}"
fi

# Preserve local defaults without exposing infrastructure values in Compose config.
: "${AWS_PROFILE:=default}"
: "${AWS_REGION:=us-east-1}"
: "${AWS_DEFAULT_REGION:=${AWS_REGION}}"
: "${OLIST_S3_PREFIX:=olist}"
: "${REDSHIFT_PORT:=5439}"

export AWS_PROFILE AWS_REGION AWS_DEFAULT_REGION OLIST_S3_PREFIX REDSHIFT_PORT

exec "$@"
