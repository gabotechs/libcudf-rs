#!/usr/bin/env bash
set -euo pipefail

stack_name=${1:-dev}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd "$script_dir"

passphrase_path=".ssh/${stack_name}.pulumi-passphrase"
if [[ -f "$passphrase_path" ]]; then
    export PULUMI_CONFIG_PASSPHRASE_FILE="$passphrase_path"
fi

pulumi stack select "$stack_name" >/dev/null
key_path=".ssh/${stack_name}.pem"

if [[ ! -f "$key_path" ]]; then
    mkdir -p .ssh
    pulumi stack output privateKey --show-secrets >"$key_path"
    chmod 0600 "$key_path"
fi

./ssh-config.sh add "$stack_name" >/dev/null
exec ssh "libcudf-dev-$stack_name"
