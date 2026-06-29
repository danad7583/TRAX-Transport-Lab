#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${TRAX_TCP_LAB_REPO_URL:-}"
REPO_BRANCH="${TRAX_TCP_LAB_REPO_BRANCH:-main}"
LAB_ROOT="${TRAX_TCP_LAB_ROOT:-/opt/trax/tcp-transport-lab}"
STATUS_DIR="${TRAX_TCP_LAB_STATUS_DIR:-/opt/trax/status}"
BOOTSTRAP_LOG="${TRAX_TCP_LAB_BOOTSTRAP_LOG:-/var/log/trax-tcp-lab-bootstrap.log}"
TEST_LOG="${TRAX_TCP_LAB_TEST_LOG:-/var/log/trax-tcp-lab-test.log}"
TRAX_CORE_REPO_URL="${TRAX_CORE_REPO_URL:-https://github.com/danad7583/TRAX.git}"
TRAX_CORE_BRANCH="${TRAX_CORE_BRANCH:-main}"
STATUS_FILE="${STATUS_DIR}/bootstrap-status.json"
VENV_DIR="${LAB_ROOT}/.venv"
PYTHON_BIN="python3.11"
PYTHON_VERSION="unknown"

mkdir -p "${STATUS_DIR}" "$(dirname "${BOOTSTRAP_LOG}")" "$(dirname "${TEST_LOG}")"
touch "${BOOTSTRAP_LOG}" "${TEST_LOG}"

exec > >(tee -a "${BOOTSTRAP_LOG}") 2>&1

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

write_status() {
  local state="$1"
  local message="$2"
  local exit_code="${3:-0}"
  local timestamp
  timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  cat > "${STATUS_FILE}" <<EOF
{
  "state": "$(json_escape "${state}")",
  "message": "$(json_escape "${message}")",
  "exit_code": ${exit_code},
  "repo_url": "$(json_escape "${REPO_URL}")",
  "repo_branch": "$(json_escape "${REPO_BRANCH}")",
  "trax_core_repo_url": "$(json_escape "${TRAX_CORE_REPO_URL}")",
  "trax_core_branch": "$(json_escape "${TRAX_CORE_BRANCH}")",
  "lab_root": "$(json_escape "${LAB_ROOT}")",
  "python_version": "$(json_escape "${PYTHON_VERSION}")",
  "bootstrap_log": "$(json_escape "${BOOTSTRAP_LOG}")",
  "test_log": "$(json_escape "${TEST_LOG}")",
  "updated_at": "${timestamp}"
}
EOF
}

on_error() {
  local exit_code="$?"
  write_status "failed" "Bootstrap failed with exit code ${exit_code}." "${exit_code}"
  exit "${exit_code}"
}
trap on_error ERR

if [ -z "${REPO_URL}" ] && [ ! -d "${LAB_ROOT}/.git" ]; then
  echo "TRAX_TCP_LAB_REPO_URL is required when ${LAB_ROOT} is not already cloned." >&2
  exit 2
fi

run_test_log() {
  (
    cd "${LAB_ROOT}"
    "$@"
  ) 2>&1 | tee -a "${TEST_LOG}"
}

install_packages() {
  if ! command -v dnf >/dev/null 2>&1; then
    echo "This bootstrap script expects Amazon Linux 2023 with dnf." >&2
    exit 1
  fi

  dnf install -y \
    git \
    gcc \
    gcc-c++ \
    make \
    pkgconf-pkg-config \
    openssl-devel \
    rust \
    cargo \
    python3.11 \
    python3.11-devel \
    python3.11-pip
}

clone_or_update_transport_lab() {
  mkdir -p "$(dirname "${LAB_ROOT}")"
  if [ ! -d "${LAB_ROOT}/.git" ]; then
    git clone --branch "${REPO_BRANCH}" "${REPO_URL}" "${LAB_ROOT}"
  else
    git -C "${LAB_ROOT}" fetch origin "${REPO_BRANCH}"
    git -C "${LAB_ROOT}" checkout "${REPO_BRANCH}"
    git -C "${LAB_ROOT}" pull --ff-only origin "${REPO_BRANCH}"
  fi
}

create_venv() {
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"
  PYTHON_VERSION="$(python --version 2>&1)"
  python -m pip install --upgrade pip setuptools wheel
  python -m pip install maturin pytest
}

clone_or_update_trax_core() {
  mkdir -p "${LAB_ROOT}/external"
  if [ ! -d "${LAB_ROOT}/external/TRAX/.git" ]; then
    git clone --branch "${TRAX_CORE_BRANCH}" "${TRAX_CORE_REPO_URL}" "${LAB_ROOT}/external/TRAX"
  else
    git -C "${LAB_ROOT}/external/TRAX" fetch origin "${TRAX_CORE_BRANCH}"
    git -C "${LAB_ROOT}/external/TRAX" checkout "${TRAX_CORE_BRANCH}"
    git -C "${LAB_ROOT}/external/TRAX" pull --ff-only origin "${TRAX_CORE_BRANCH}"
  fi
}

build_trax_core() {
  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"
  (
    cd "${LAB_ROOT}/external/TRAX"
    maturin develop
  )
  python -c "import trax; print('trax import OK')"
}

install_transport_lab() {
  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"
  (
    cd "${LAB_ROOT}"
    if [ -f requirements.txt ]; then
      python -m pip install -r requirements.txt
    fi
    python -m pip install -e .
  )
}

run_validation() {
  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"
  run_test_log python -m pytest
  run_test_log python -m trax_transport_lab.tcp_demo --mode dag-genesis --messages 10
  run_test_log python -m trax_transport_lab.udp_demo --mode dag-genesis --messages 10
}

write_status "running" "Installing Amazon Linux 2023 dependencies and preparing TCP Transport Lab."
install_packages
clone_or_update_transport_lab
create_venv
clone_or_update_trax_core
build_trax_core
install_transport_lab
write_status "running" "Running TCP Transport Lab pytest and DAG-genesis smoke validation."
run_validation
write_status "succeeded" "Bootstrap succeeded: TCP Transport Lab cloud validation completed." 0
