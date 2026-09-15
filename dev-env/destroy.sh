#!/usr/bin/env bash
set -euo pipefail

stack_name=${1:-dev}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd "$script_dir"

passphrase_path=".ssh/${stack_name}.pulumi-passphrase"
if [[ -f "$passphrase_path" ]]; then
    export PULUMI_CONFIG_PASSPHRASE_FILE="$passphrase_path"
fi

pulumi stack select "$stack_name"
pulumi destroy
./ssh-config.sh remove "$stack_name"
