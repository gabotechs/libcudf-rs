#!/usr/bin/env bash
set -euo pipefail

action=${1:-}
stack_name=${2:-dev}
if [[ "$action" != "add" && "$action" != "remove" ]]; then
    echo "Usage: $0 <add|remove> [stack-name]" >&2
    exit 1
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd "$script_dir"

host_alias="libcudf-dev-$stack_name"
begin_marker="# BEGIN libcudf-dev-env $host_alias"
end_marker="# END libcudf-dev-env $host_alias"
ssh_dir="${HOME}/.ssh"
config_file="$ssh_dir/config"

mkdir -p "$ssh_dir"
chmod 0700 "$ssh_dir"
touch "$config_file"

temporary_config=$(mktemp "$ssh_dir/.libcudf-config.XXXXXX")
trap 'rm -f "$temporary_config"' EXIT

awk -v begin="$begin_marker" -v end="$end_marker" '
    $0 == begin { skipping = 1; next }
    $0 == end { skipping = 0; next }
    !skipping { lines[++count] = $0 }
    END {
        while (count > 0 && lines[count] == "") count--
        for (line = 1; line <= count; line++) print lines[line]
    }
' "$config_file" >"$temporary_config"

if [[ "$action" == "add" ]]; then
    passphrase_path=".ssh/${stack_name}.pulumi-passphrase"
    if [[ -f "$passphrase_path" ]]; then
        export PULUMI_CONFIG_PASSPHRASE_FILE="$passphrase_path"
    fi
    pulumi stack select "$stack_name" >/dev/null
    public_dns=$(pulumi stack output publicDns)
    key_path="$script_dir/.ssh/${stack_name}.pem"
    if [[ ! -f "$key_path" ]]; then
        echo "Missing private key: $key_path" >&2
        exit 1
    fi

    if [[ -s "$temporary_config" ]]; then
        printf '\n' >>"$temporary_config"
    fi
    printf '%s\n' \
        "$begin_marker" \
        "Host $host_alias" \
        "    HostName $public_dns" \
        "    User ubuntu" \
        "    IdentityFile $key_path" \
        "    IdentitiesOnly yes" \
        "    StrictHostKeyChecking accept-new" \
        "    UserKnownHostsFile $script_dir/.ssh/known_hosts" \
        "$end_marker" >>"$temporary_config"
fi

chmod 0600 "$temporary_config"
mv "$temporary_config" "$config_file"
trap - EXIT

if [[ "$action" == "add" ]]; then
    echo "$host_alias"
fi
