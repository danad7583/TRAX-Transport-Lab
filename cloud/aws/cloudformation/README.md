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

## Validate IaC Locally

Run from the repository root:

```bash
bash cloud/aws/cloudformation/scripts/validate-trax-iac.sh
```

If the AWS CLI is installed and configured, the validation script also runs `aws cloudformation validate-template` against:

```bash
cloud/aws/cloudformation/trax-tcp-lab-ec2.yaml
```
