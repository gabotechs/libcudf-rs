#!/usr/bin/env bash
set -euo pipefail

stack_name=${1:-dev}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd -- "$script_dir/.." && pwd)
cd "$script_dir"

passphrase_path=".ssh/${stack_name}.pulumi-passphrase"
if [[ -f "$passphrase_path" ]]; then
    export PULUMI_CONFIG_PASSPHRASE_FILE="$passphrase_path"
fi

pulumi stack select "$stack_name" >/dev/null
host=$(pulumi stack output publicDns)
key_path="$script_dir/.ssh/${stack_name}.pem"

if [[ ! -f "$key_path" ]]; then
    mkdir -p .ssh
    pulumi stack output privateKey --show-secrets >"$key_path"
    chmod 0600 "$key_path"
fi

ssh_options="ssh -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=$script_dir/.ssh/known_hosts -i $key_path"
rsync -az \
    --exclude .git/ \
    --exclude target/ \
    --exclude dev-env/.npm-cache/ \
    --exclude dev-env/node_modules/ \
    --exclude dev-env/.ssh/ \
    --exclude 'dev-env/Pulumi.*.yaml' \
    -e "$ssh_options" \
    "$repo_dir/" "ubuntu@$host:/home/ubuntu/libcudf-rs/"

ssh -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$script_dir/.ssh/known_hosts" -i "$key_path" "ubuntu@$host" \
    "bash -lc 'cd /home/ubuntu/libcudf-rs && cargo test --workspace'"
