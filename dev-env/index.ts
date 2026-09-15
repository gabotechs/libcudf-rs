import * as aws from "@pulumi/aws";
import * as pulumi from "@pulumi/pulumi";
import * as tls from "@pulumi/tls";

const config = new pulumi.Config();
const awsConfig = new pulumi.Config("aws");

const region = awsConfig.get("region") ?? "us-east-1";
const instanceType = config.get("instanceType") ?? "g7.4xlarge";
const sshAllowedCidr = config.require("sshAllowedCidr");
const rootVolumeSize = config.getNumber("rootVolumeSize") ?? 250;

const provider = new aws.Provider("aws", {
    region: region as aws.Region,
});

const providerOptions = { provider };
const tags = {
    Project: "libcudf-rs",
    Environment: "development",
    "pulumi:stack": pulumi.getStack(),
};

const offerings = aws.ec2.getInstanceTypeOfferingsOutput(
    {
        filters: [{ name: "instance-type", values: [instanceType] }],
        locationType: "availability-zone",
    },
    providerOptions,
);

const availabilityZone = offerings.locations.apply((locations) => {
    const sortedLocations = [...locations].sort();
    if (sortedLocations.length === 0) {
        throw new Error(
            `${instanceType} is not offered in ${region}; choose another aws:region or instanceType`,
        );
    }
    return sortedLocations[0];
});

const vpc = new aws.ec2.Vpc(
    "vpc",
    {
        cidrBlock: "10.42.0.0/16",
        enableDnsHostnames: true,
        enableDnsSupport: true,
        tags: { ...tags, Name: `libcudf-dev-${pulumi.getStack()}` },
    },
    providerOptions,
);

const internetGateway = new aws.ec2.InternetGateway(
    "internet-gateway",
    {
        vpcId: vpc.id,
        tags,
    },
    providerOptions,
);

const subnet = new aws.ec2.Subnet(
    "public-subnet",
    {
        availabilityZone,
        cidrBlock: "10.42.0.0/24",
        mapPublicIpOnLaunch: true,
        vpcId: vpc.id,
        tags: { ...tags, Name: `libcudf-dev-${pulumi.getStack()}-public` },
    },
    providerOptions,
);

const routeTable = new aws.ec2.RouteTable(
    "public-routes",
    {
        routes: [{ cidrBlock: "0.0.0.0/0", gatewayId: internetGateway.id }],
        vpcId: vpc.id,
        tags,
    },
    providerOptions,
);

new aws.ec2.RouteTableAssociation(
    "public-route-association",
    {
        routeTableId: routeTable.id,
        subnetId: subnet.id,
    },
    providerOptions,
);

const securityGroup = new aws.ec2.SecurityGroup(
    "ssh",
    {
        description: "SSH access to the libcudf-rs development machine",
        ingress: [
            {
                cidrBlocks: [sshAllowedCidr],
                description: "SSH from the developer laptop current public IP",
                fromPort: 22,
                protocol: "tcp",
                toPort: 22,
            },
        ],
        egress: [
            {
                cidrBlocks: ["0.0.0.0/0"],
                fromPort: 0,
                protocol: "-1",
                toPort: 0,
            },
        ],
        vpcId: vpc.id,
        tags,
    },
    providerOptions,
);

const sshKey = new tls.PrivateKey("ssh-key", {
    algorithm: "RSA",
    rsaBits: 4096,
});

const keyPair = new aws.ec2.KeyPair(
    "ssh-key",
    {
        publicKey: sshKey.publicKeyOpenssh,
        tags,
    },
    providerOptions,
);

const dlami = aws.ssm.getParameterOutput(
    {
        name: "/aws/service/deeplearning/ami/x86_64/base-oss-nvidia-driver-gpu-ubuntu-24.04/latest/ami-id",
    },
    providerOptions,
);

const userData = `#!/usr/bin/env bash
set -euxo pipefail
exec > >(tee /var/log/libcudf-dev-setup.log | logger -t libcudf-dev-setup -s 2>/dev/console) 2>&1

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends \\
    build-essential \\
    ca-certificates \\
    cmake \\
    curl \\
    g++-13 \\
    gcc-13 \\
    gh \\
    git \\
    libssl-dev \\
    ninja-build \\
    pkg-config \\
    protobuf-compiler \\
    rsync \\
    unzip \\
    zip

update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-13 130
update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-13 130

test -d /usr/local/cuda-12.9
ln -sfn /usr/local/cuda-12.9 /usr/local/cuda

cat >/etc/profile.d/libcudf-dev.sh <<'PROFILE'
export CUDA_ROOT=/usr/local/cuda
export CUDA_HOME=/usr/local/cuda
export PATH=/home/ubuntu/.cargo/bin:/usr/local/cuda/bin:$PATH
PROFILE
chmod 0644 /etc/profile.d/libcudf-dev.sh

install -d -o ubuntu -g ubuntu /home/ubuntu/libcudf-rs
sudo -u ubuntu env HOME=/home/ubuntu RUSTUP_HOME=/home/ubuntu/.rustup CARGO_HOME=/home/ubuntu/.cargo \\
    sh -c 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal'
sudo -u ubuntu env HOME=/home/ubuntu RUSTUP_HOME=/home/ubuntu/.rustup CARGO_HOME=/home/ubuntu/.cargo \\
    /home/ubuntu/.cargo/bin/rustup component add clippy rustfmt

/usr/bin/nvidia-smi
/usr/local/cuda/bin/nvcc --version
sudo -iu ubuntu rustc --version
sudo -iu ubuntu cargo --version
gh --version
g++ --version

touch /var/lib/libcudf-dev-ready
`;

const instance = new aws.ec2.Instance(
    "development-machine",
    {
        ami: dlami.value,
        associatePublicIpAddress: true,
        availabilityZone,
        instanceType,
        keyName: keyPair.keyName,
        metadataOptions: {
            httpEndpoint: "enabled",
            httpTokens: "required",
        },
        rootBlockDevice: {
            deleteOnTermination: true,
            encrypted: true,
            volumeSize: rootVolumeSize,
            volumeType: "gp3",
        },
        subnetId: subnet.id,
        userData,
        userDataReplaceOnChange: true,
        vpcSecurityGroupIds: [securityGroup.id],
        tags: { ...tags, Name: `libcudf-dev-${pulumi.getStack()}` },
    },
    providerOptions,
);

export const amiId = dlami.value;
export { availabilityZone, instanceType, region };
export const instanceId = instance.id;
export const publicDns = instance.publicDns;
export const publicIp = instance.publicIp;
export const sshUser = "ubuntu";
export const privateKey = pulumi.secret(sshKey.privateKeyPem);
export const sshHostAlias = `libcudf-dev-${pulumi.getStack()}`;
export const sshCommand = `ssh ${sshHostAlias}`;
