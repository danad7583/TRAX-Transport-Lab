# TCP Transport Lab AWS CloudFormation

This folder contains AWS CloudFormation support for the TCP Transport Lab cloud validation phase. It provisions one persistent Linux EC2 instance that pulls, builds, and runs this same TCP/UDP Transport Lab repository.

The repository split is:

```text
trax_transport_lab/
  Python lab package and TCP/UDP demo logic.

tests/
  Local and CI tests for lab behavior.

scripts/
  Local lab runner scripts such as run_all.py, scale_messages.py, compare_modes.py.

cloud/
  Cloud provider support for running the lab on persistent infrastructure.

cloud/aws/cloudformation/
  AWS CloudFormation templates and bootstrap scripts for persistent EC2 lab testing.
```

No CloudFormation files belong under `trax_transport_lab/`, `tests/`, or the normal lab `scripts/` directory.

## Template

```text
cloud/aws/cloudformation/trax-tcp-lab-ec2.yaml
```

The stack creates:

- One persistent Linux EC2 instance.
- One security group allowing SSH only from `AllowedSshCidr`.
- One EC2 instance role/profile for AWS Systems Manager access.

No ECS, EKS, Docker, Kubernetes, autoscaling group, or load balancer is added for this milestone.

SSM Session Manager is the preferred access path for this lab. The stack still accepts `KeyName` for SSH fallback, but EC2 private key files must remain local operator secrets. Never commit `.pem` files or private keys to this repository.

## Deploy

Run from the repository root:

```bash
aws cloudformation deploy \
  --template-file cloud/aws/cloudformation/trax-tcp-lab-ec2.yaml \
  --stack-name trax-tcp-lab \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    VpcId=vpc-xxxx \
    SubnetId=subnet-xxxx \
    KeyName=my-key \
    AllowedSshCidr=x.x.x.x/32 \
    RepoUrl=https://github.com/<owner>/<tcp-lab-repo>.git \
    RepoBranch=main
```

## Bootstrap Behavior

The EC2 bootstrap flow:

1. Installs Amazon Linux 2023 build/runtime dependencies.
2. Clones this TCP Transport Lab repository.
3. Uses Python 3.11 because Amazon Linux 2023's default `python3` is currently Python 3.9 and the lab requires Python >= 3.10.
4. Creates a Python virtual environment with `python3.11 -m venv .venv`.
5. Installs `pip`, `setuptools`, `wheel`, `maturin`, and `pytest`.
6. Clones TRAX Core from `https://github.com/danad7583/TRAX.git` into `external/TRAX`.
7. Builds and installs the required `trax` Python module into the active venv with `maturin develop`.
8. Installs the TCP Transport Lab package in editable mode.
9. Runs `python -m pytest`.
10. Runs TCP and UDP DAG-genesis smoke demos with 10 messages each.
11. Verifies the expected DAG-genesis trust model in the smoke output: one signed genesis create, one signed genesis verify, and zero hot-path signed packets.
12. Saves logs under `/var/log/trax-tcp-lab-bootstrap.log` and `/var/log/trax-tcp-lab-test.log`.
13. Writes status to `/opt/trax/status/bootstrap-status.json`.

Expected fresh EC2 validation:

- `python -m pytest` passes with 108 tests.
- `python -m trax_transport_lab.tcp_demo --mode dag-genesis --messages 10` passes.
- `python -m trax_transport_lab.udp_demo --mode dag-genesis --messages 10` passes.
- DAG-genesis metrics show:
  - `signed_genesis_create_count: 1`
  - `signed_genesis_verify_count: 1`
  - `hot_path_signed_packet_count: 0`

Local loopback and single-node cloud metrics are diagnostic only. They are useful for validation and regression tracking, not benchmark-grade performance claims.

## Logs and Status

The bootstrap writes:

- `/var/log/trax-tcp-lab-bootstrap.log`
- `/var/log/trax-tcp-lab-test.log`
- `/opt/trax/status/bootstrap-status.json`

Check status through SSM:

```bash
sudo cat /opt/trax/status/bootstrap-status.json
```

Review logs through SSM:

```bash
sudo tail -200 /var/log/trax-tcp-lab-bootstrap.log
sudo tail -200 /var/log/trax-tcp-lab-test.log
```

## EC2-to-EC2 Bidirectional TCP Peer Session

This lab uses Option A: update the existing CloudFormation stack, then pull the updated repository on both already-running EC2 instances. Do not delete and recreate the stack for this step.

Discover the private IPs for the two running lab instances:

```bash
aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=trax-tcp-lab" "Name=instance-state-name,Values=running" \
  --query "Reservations[].Instances[].{InstanceId:InstanceId,PrivateIp:PrivateIpAddress,PublicIp:PublicIpAddress,AZ:Placement.AvailabilityZone}" \
  --output table
```

Update the existing stack from the repository root. The template path in this repo is `cloud/aws/cloudformation/trax-tcp-lab-ec2.yaml`:

```powershell
aws cloudformation deploy `
  --stack-name trax-tcp-lab-ec2-v2 `
  --template-file cloud/aws/cloudformation/trax-tcp-lab-ec2.yaml `
  --capabilities CAPABILITY_NAMED_IAM
```

The stack update adds private EC2-to-EC2 TCP ingress on port `39100` through a security-group self-reference. It does not expose port `39100` to `0.0.0.0/0`.

After the repo changes are pushed, update both EC2 instances through SSM:

```bash
cd /opt/trax/tcp-transport-lab
git pull origin main
source .venv/bin/activate
python -m pip install -e .
python -m pytest
```

Start the receiver on EC2-B first:

```bash
cd /opt/trax/tcp-transport-lab
source .venv/bin/activate

python -m trax_transport_lab.tcp_peer \
  --node-id ec2-b \
  --listen-host 0.0.0.0 \
  --listen-port 39100 \
  --peer-host <ec2-a-private-ip> \
  --peer-port 39100 \
  --mode dag-genesis \
  --key-mode shared \
  --json
```

Start the initiator on EC2-A second:

```bash
cd /opt/trax/tcp-transport-lab
source .venv/bin/activate

python -m trax_transport_lab.tcp_peer \
  --node-id ec2-a \
  --listen-host 0.0.0.0 \
  --listen-port 39100 \
  --peer-host <ec2-b-private-ip> \
  --peer-port 39100 \
  --mode dag-genesis \
  --duration-seconds 60 \
  --payload-size 1048576 \
  --chunk-size 1400 \
  --dag-signing-cadence 8 \
  --agent-key-rotation-cadence 1000 \
  --dag-key-rotation-cadence 10000 \
  --key-mode shared \
  --max-dag-nodes 100000 \
  --initiator \
  --json
```

Optional larger run from EC2-A:

```bash
python -m trax_transport_lab.tcp_peer \
  --node-id ec2-a \
  --listen-host 0.0.0.0 \
  --listen-port 39100 \
  --peer-host <ec2-b-private-ip> \
  --peer-port 39100 \
  --mode dag-genesis \
  --duration-seconds 60 \
  --payload-size 67108864 \
  --chunk-size 1400 \
  --dag-signing-cadence 8 \
  --agent-key-rotation-cadence 1000 \
  --dag-key-rotation-cadence 10000 \
  --key-mode shared \
  --max-dag-nodes 100000 \
  --initiator \
  --json
```

Expected result:

- Both peers report `ok: true`.
- Receiver reports `genesis_start_received: true`.
- Initiator reports `genesis_accept_received: true`.
- Receiver reports `genesis_ready_received: true`.
- Receiver accepted DAG config matches the initiator config.
- Initiator reports `final_response_received: true`.
- Receiver reports `final_response_sent: true`.
- Traffic stop ends demo traffic only.
- No DAG close/finalize semantics are reported.
- `hot_path_signed_packet_count` remains `0`.
- `signed_genesis_create_count` remains `1`.
- `signed_genesis_verify_count` remains `1`.

## Validate IaC Locally

Run from the repository root:

```bash
bash cloud/aws/cloudformation/scripts/validate-trax-iac.sh
```

If the AWS CLI is installed and configured, the validation script also runs `aws cloudformation validate-template` against:

```bash
cloud/aws/cloudformation/trax-tcp-lab-ec2.yaml
```
