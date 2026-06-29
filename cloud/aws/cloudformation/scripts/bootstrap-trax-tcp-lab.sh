#!/usr/bin/env bash
set -Eeuo pipefail

REPO_URL="${TRAX_TCP_LAB_REPO_URL:-}"
REPO_BRANCH="${TRAX_TCP_LAB_REPO_BRANCH:-main}"
LAB_ROOT="${TRAX_TCP_LAB_ROOT:-/opt/trax/tcp-transport-lab}"
STATUS_DIR="${TRAX_TCP_LAB_STATUS_DIR:-/opt/trax/status}"
BOOTSTRAP_LOG="${TRAX_TCP_LAB_BOOTSTRAP_LOG:-/var/log/trax-tcp-lab-bootstrap.log}"
TEST_LOG="${TRAX_TCP_LAB_TEST_LOG:-/var/log/trax-tcp-lab-test.log}"
STATUS_FILE="${STATUS_DIR}/bootstrap-status.json"
VENV_DIR="${LAB_ROOT}/.venv"

mkdir -p "${STATUS_DIR}" "$(dirname "${BOOTSTRAP_LOG}")" "$(dirname "${TEST_LOG}")"
touch "${BOOTSTRAP_LOG}" "${TEST_LOG}"

if [ -z "${REPO_URL}" ] && [ ! -d "${LAB_ROOT}/.git" ]; then
  echo "TRAX_TCP_LAB_REPO_URL is required when ${LAB_ROOT} is not already cloned." >&2
  exit 2
fi

exec > >(tee -a "${BOOTSTRAP_LOG}") 2>&1

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

write_status() {
  local state="$1"
  local message="$2"
  local timestamp
  timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  cat > "${STATUS_FILE}" <<EOF
{
  "state": "$(json_escape "${state}")",
  "message": "$(json_escape "${message}")",
  "repo_url": "$(json_escape "${REPO_URL}")",
  "repo_branch": "$(json_escape "${REPO_BRANCH}")",
  "lab_root": "$(json_escape "${LAB_ROOT}")",
  "bootstrap_log": "$(json_escape "${BOOTSTRAP_LOG}")",
  "test_log": "$(json_escape "${TEST_LOG}")",
  "updated_at": "${timestamp}"
}
EOF
}

on_error() {
  local exit_code="$?"
  write_status "failed" "Bootstrap failed with exit code ${exit_code}."
  exit "${exit_code}"
}
trap on_error ERR

install_packages() {
  if command -v dnf >/dev/null 2>&1; then
    dnf install -y git gcc gcc-c++ make openssl-devel pkgconf-pkg-config python3 python3-devel python3-pip rust cargo
  elif command -v yum >/dev/null 2>&1; then
    yum install -y git gcc gcc-c++ make openssl-devel pkgconfig python3 python3-devel python3-pip rust cargo
  elif command -v apt-get >/dev/null 2>&1; then
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y git build-essential pkg-config libssl-dev python3 python3-dev python3-pip python3-venv rustc cargo
  else
    echo "No supported package manager found." >&2
    exit 1
  fi
}

clone_or_update_repo() {
  mkdir -p "$(dirname "${LAB_ROOT}")"
  if [ ! -d "${LAB_ROOT}/.git" ]; then
    git clone --branch "${REPO_BRANCH}" "${REPO_URL}" "${LAB_ROOT}"
  else
    git -C "${LAB_ROOT}" fetch origin "${REPO_BRANCH}" || true
    git -C "${LAB_ROOT}" checkout "${REPO_BRANCH}" || true
    git -C "${LAB_ROOT}" pull --ff-only origin "${REPO_BRANCH}" || true
  fi
}

create_venv() {
  python3 -m venv "${VENV_DIR}"
  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"
  python -m pip install --upgrade pip
  python -m pip install -e "${LAB_ROOT}"
  if [ -f "${LAB_ROOT}/requirements.txt" ]; then
    python -m pip install -r "${LAB_ROOT}/requirements.txt"
  fi
  python -m pip install maturin pytest
}

build_trax_bindings() {
  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"
  python "${LAB_ROOT}/scripts/bootstrap_trax.py"
}

run_lab_validation() {
  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"
  (
    cd "${LAB_ROOT}"
    python scripts/run_all.py
  ) 2>&1 | tee -a "${TEST_LOG}"
}

write_status "running" "Installing dependencies and preparing TCP Transport Lab."
install_packages
clone_or_update_repo
create_venv
build_trax_bindings
write_status "running" "Running TCP Transport Lab validation."
run_lab_validation
write_status "succeeded" "TCP Transport Lab cloud validation completed successfully."
