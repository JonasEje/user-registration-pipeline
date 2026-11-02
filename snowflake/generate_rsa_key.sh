#!/usr/bin/env bash
set -euo pipefail

key_directory="${1:-$HOME/.snowflake}"
key_name="${2:-rsa_key}"

if ! command -v openssl >/dev/null 2>&1; then
    echo "OpenSSL not found. Install it with your package manager." >&2
    exit 1
fi

mkdir -p "$key_directory"

private_key_path="$key_directory/$key_name.p8"
public_key_path="$key_directory/$key_name.pub"

if [ -e "$private_key_path" ]; then
    echo "Refusing to overwrite existing key: $private_key_path" >&2
    exit 1
fi

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$private_key_path"
openssl pkey -in "$private_key_path" -pubout -out "$public_key_path"

public_key_body="$(grep -v "PUBLIC KEY" "$public_key_path" | tr -d '[:space:]')"

printf 'Private key: %s\n' "$private_key_path"
printf 'Public key:  %s\n' "$public_key_path"
printf '\n'
printf 'Register the public key in Snowflake as ACCOUNTADMIN:\n'
printf '\n'
printf "ALTER USER <your-snowflake-user> SET RSA_PUBLIC_KEY='%s';\n" "$public_key_body"
printf '\n'
printf 'Then add this line to .env and remove SNOWFLAKE_PASSWORD:\n'
printf '\n'
printf 'SNOWFLAKE_PRIVATE_KEY_PATH=%s\n' "$private_key_path"
