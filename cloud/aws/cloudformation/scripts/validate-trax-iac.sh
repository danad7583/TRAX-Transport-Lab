#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
TEMPLATE="${ROOT}/cloud/aws/cloudformation/trax-tcp-lab-ec2.yaml"
README="${ROOT}/cloud/aws/cloudformation/README.md"
BOOTSTRAP="${ROOT}/cloud/aws/cloudformation/scripts/bootstrap-trax-tcp-lab.sh"
VALIDATE="${ROOT}/cloud/aws/cloudformation/scripts/validate-trax-iac.sh"

required_files=(
  "${TEMPLATE}"
  "${BOOTSTRAP}"
  "${VALIDATE}"
  "${README}"
)

for file in "${required_files[@]}"; do
  if [ ! -f "${file}" ]; then
    echo "Missing required IaC file: ${file#${ROOT}/}" >&2
    exit 1
  fi
done

bash -n "${BOOTSTRAP}"
bash -n "${VALIDATE}"

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
