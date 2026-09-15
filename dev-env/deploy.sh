#!/usr/bin/env bash
set -euo pipefail

stack_name=${1:-dev}
region=${2:-us-east-1}

if [[ -z "${AWS_PROFILE:-}" ]]; then
    echo "Set AWS_PROFILE to the SSO profile that should own this environment." >&2
    exit 1
fi

for command in aws curl npm openssl pulumi ssh; do
    if ! command -v "$command" >/dev/null; then
        echo "Missing required command: $command" >&2
        exit 1
    fi
done

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd "$script_dir"

aws sso login --profile "$AWS_PROFILE"

region_status=$(
    aws account get-region-opt-status \
        --profile "$AWS_PROFILE" \
        --region-name "$region" \
        --query RegionOptStatus \
        --output text
)
if [[ "$region_status" == "DISABLED" || "$region_status" == "DISABLING" ]]; then
    echo "AWS region $region is $region_status for this account; enable it or choose another region." >&2
    exit 1
fi
if [[ "$region_status" == "ENABLING" ]]; then
    echo "AWS region $region is still enabling; wait until it is enabled and retry." >&2
    exit 1
fi

# The Pulumi AWS provider does not consistently consume AWS CLI v2's SSO cache.
# Export the short-lived credentials obtained from this profile into this process.
credential_json=$(aws configure export-credentials --profile "$AWS_PROFILE" --format process)
read -r AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN < <(
    node -e '
        const chunks = [];
        process.stdin.on("data", chunk => chunks.push(chunk));
        process.stdin.on("end", () => {
            const value = JSON.parse(Buffer.concat(chunks).toString());
            console.log([value.AccessKeyId, value.SecretAccessKey, value.SessionToken].join(" "));
        });
    ' <<<"$credential_json"
)
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
unset AWS_PROFILE AWS_CREDENTIAL_EXPIRATION AWS_ASSUME_ROLE_TTL AWS_SESSION_TTL AWS_VAULT_KEYCHAIN_NAME

if [[ ! -d node_modules ]]; then
    npm ci --cache .npm-cache
fi

mkdir -p .ssh
chmod 0700 .ssh
passphrase_path=".ssh/${stack_name}.pulumi-passphrase"
if [[ ! -f "$passphrase_path" ]]; then
    openssl rand -hex 32 >"$passphrase_path"
    chmod 0600 "$passphrase_path"
fi
export PULUMI_CONFIG_PASSPHRASE_FILE="$passphrase_path"

pulumi stack select --create "$stack_name"
pulumi config rm aws:profile >/dev/null 2>&1 || true
pulumi config set aws:region "$region"

public_ip=$(curl --fail --silent --show-error https://checkip.amazonaws.com | tr -d '[:space:]')
if [[ ! "$public_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Could not determine this laptop's public IPv4 address." >&2
    exit 1
fi
pulumi config set sshAllowedCidr "$public_ip/32"

npm run check
pulumi up

key_path=".ssh/${stack_name}.pem"
pulumi stack output privateKey --show-secrets >"$key_path"
chmod 0600 "$key_path"

host=$(pulumi stack output publicDns)
echo "Waiting for cloud-init on $host..."
ssh -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=.ssh/known_hosts -o ConnectTimeout=10 -i "$key_path" "ubuntu@$host" \
    'cloud-init status --wait && test -f /var/lib/libcudf-dev-ready'

./ssh-config.sh add "$stack_name"

echo "Development machine is ready."
echo "Connect with: ssh libcudf-dev-$stack_name"
echo "Copy this checkout and run all tests with: ./sync-and-test.sh $stack_name"
