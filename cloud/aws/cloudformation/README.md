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

1. Installs Linux build/runtime dependencies.
2. Clones this TCP Transport Lab repository.
3. Creates a Python virtual environment.
4. Installs Python dependencies.
5. Builds/installs TRAX Python bindings through the lab's existing `scripts/bootstrap_trax.py`.
6. Runs the existing TCP lab validation command, `python scripts/run_all.py`.
7. Saves logs under `/var/log/trax-tcp-lab-bootstrap.log` and `/var/log/trax-tcp-lab-test.log`.
8. Writes status to `/opt/trax/status/bootstrap-status.json`.

## Validate IaC Locally

Run from the repository root:

```bash
bash cloud/aws/cloudformation/scripts/validate-trax-iac.sh
```

If the AWS CLI is installed and configured, the validation script also runs `aws cloudformation validate-template` against:

```bash
cloud/aws/cloudformation/trax-tcp-lab-ec2.yaml
```
