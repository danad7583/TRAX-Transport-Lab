#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
TEMPLATE="${ROOT}/cloud/aws/cloudformation/trax-tcp-lab-ec2.yaml"
README="${ROOT}/cloud/aws/cloudformation/README.md"
BOOTSTRAP="${ROOT}/cloud/aws/cloudformation/scripts/bootstrap-trax-tcp-lab.sh"
VALIDATE="${ROOT}/cloud/aws/cloudformation/scripts/validate-trax-iac.sh"
GITIGNORE="${ROOT}/.gitignore"

required_files=(
  "${TEMPLATE}"
  "${BOOTSTRAP}"
  "${VALIDATE}"
  "${README}"
  "${GITIGNORE}"
)

for file in "${required_files[@]}"; do
  if [ ! -f "${file}" ]; then
    echo "Missing required IaC file: ${file#${ROOT}/}" >&2
    exit 1
  fi
done

bash -n "${BOOTSTRAP}"
bash -n "${VALIDATE}"

require_contains() {
  local file="$1"
  local pattern="$2"
  local message="$3"
  if ! grep -Eq "${pattern}" "${file}"; then
    echo "${message}" >&2
    exit 1
  fi
}

if find "${ROOT}/src/trax_transport_lab" "${ROOT}/tests" "${ROOT}/scripts" \
  -type f \( -name '*.yaml' -o -name '*.yml' -o -name '*cloudformation*' \) | grep -q .; then
  echo "IaC files must not live under src/trax_transport_lab, tests, or scripts." >&2
  exit 1
fi

if find "${ROOT}" -path "${ROOT}/cloud" -prune -o \
  -type f \( -name '*trax-tcp-lab-ec2.yaml' -o -name '*bootstrap-trax-tcp-lab.sh' \) -print | grep -q .; then
  echo "TCP lab IaC files were found outside cloud/." >&2
  exit 1
fi

grep -q 'cloud/aws/cloudformation/trax-tcp-lab-ec2.yaml' "${README}"
grep -q 'TCP Transport Lab cloud validation phase' "${README}"
grep -q 'No ECS, EKS, Docker, Kubernetes, autoscaling group, or load balancer' "${README}"

require_contains "${BOOTSTRAP}" 'set -euo pipefail' "Bootstrap script must use: set -euo pipefail."
require_contains "${BOOTSTRAP}" 'python3\.11' "Bootstrap script must use python3.11 on Amazon Linux 2023."
require_contains "${BOOTSTRAP}" 'https://github\.com/danad7583/TRAX\.git' "Bootstrap script must clone TRAX Core from the expected repo."
require_contains "${BOOTSTRAP}" 'external/TRAX' "Bootstrap script must build TRAX Core from external/TRAX."
require_contains "${BOOTSTRAP}" 'maturin develop' "Bootstrap script must build/install TRAX Core with maturin develop."
require_contains "${BOOTSTRAP}" 'python -m pytest' "Bootstrap script must run python -m pytest."
require_contains "${BOOTSTRAP}" 'trax_transport_lab\.tcp_demo --mode dag-genesis --messages 10' "Bootstrap script must run the TCP DAG-genesis smoke test."
require_contains "${BOOTSTRAP}" 'trax_transport_lab\.udp_demo --mode dag-genesis --messages 10' "Bootstrap script must run the UDP DAG-genesis smoke test."
require_contains "${GITIGNORE}" '^\*\.pem$' ".gitignore must ignore *.pem private key files."
require_contains "${README}" 'SSM Session Manager is the preferred access path' "CloudFormation README must document SSM as the preferred access path."
require_contains "${README}" 'Never commit `\.pem` files or private keys' "CloudFormation README must warn against committing private keys."

if git -C "${ROOT}" ls-files --error-unmatch trax-tcp-lab-key.pem >/dev/null 2>&1; then
  echo "trax-tcp-lab-key.pem must not be tracked by Git." >&2
  exit 1
fi

if grep -Eiq 'AWS::ECS|AWS::EKS|AWS::ElasticLoadBalancing|AWS::ElasticLoadBalancingV2|AWS::AutoScaling|Docker|Kubernetes' "${TEMPLATE}"; then
  echo "This milestone must not introduce container orchestration, autoscaling, or load balancers." >&2
  exit 1
fi

if command -v aws >/dev/null 2>&1; then
  aws_region="${AWS_REGION:-${AWS_DEFAULT_REGION:-}}"
  if [ -z "${aws_region}" ]; then
    aws_region="$(aws configure get region 2>/dev/null || true)"
  fi
  if [ -n "${aws_region}" ]; then
    template_body="file://${TEMPLATE}"
    if command -v cygpath >/dev/null 2>&1; then
      template_body="file://$(cygpath -w "${TEMPLATE}")"
    fi
    aws cloudformation validate-template --template-body "${template_body}" >/dev/null
  else
    echo "aws CLI has no configured region; skipped cloudformation validate-template."
  fi
else
  echo "aws CLI not found; skipped cloudformation validate-template."
fi

echo "TRAX TCP lab IaC validation passed."
