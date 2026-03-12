#!/usr/bin/env bash
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ISO="$PROJECT/.isolation"
ISO_HOME="$ISO/home"
TOOLS_DIR="$ISO/tools"
UV_HOME="$TOOLS_DIR/uv"
UV_BIN="$UV_HOME/bin/uv"
JDK_DIR="$ISO/jdk"
PYTHON_INSTALL_DIR="$ISO/python"
UV_CACHE_DIR="$ISO/uv-cache"
PIP_CACHE_DIR="$ISO/pip-cache"
TMP_DIR="$ISO/tmp"
LOG_DIR="$ISO/logs"
RCFILE="$TMP_DIR/isolated_bashrc"

HOST_HOME="${HOME:?HOME must be set}"
BOOTSTRAP_UV_BIN="${BOOTSTRAP_UV_BIN:-$HOST_HOME/.local/bin/uv}"
PYTHON_VERSION="${PYTHON_VERSION:-3.12.11}"
PYFLINK_PYPI_SPEC="${PYFLINK_PYPI_SPEC:-apache-flink==2.2.0}"
PYTHON_SERIES="$(printf '%s' "$PYTHON_VERSION" | cut -d. -f1,2)"
PYTHON_TAG="${PYTHON_SERIES//./}"
VENV_DIR="$ISO/venv/py$PYTHON_TAG"

XDG_CACHE_HOME="$ISO_HOME/.cache"
XDG_CONFIG_HOME="$ISO_HOME/.config"
XDG_DATA_HOME="$ISO_HOME/.local/share"
JDK_URL="https://api.adoptium.net/v3/binary/latest/17/ga/mac/aarch64/jdk/hotspot/normal/eclipse?project=jdk"
JDK_ARCHIVE="$JDK_DIR/jdk17.tar.gz"
LEGACY_SOURCE_PTH="$VENV_DIR/lib/python$PYTHON_SERIES/site-packages/flink-python-source-tree.pth"

msg() { printf '[flink-iso] %s\n' "$*"; }
die() { printf '[flink-iso] ERROR: %s\n' "$*" >&2; exit 1; }

ensure_dirs() {
  mkdir -p \
    "$ISO_HOME" \
    "$TOOLS_DIR" \
    "$JDK_DIR" \
    "$PYTHON_INSTALL_DIR" \
    "$UV_CACHE_DIR" \
    "$PIP_CACHE_DIR" \
    "$TMP_DIR" \
    "$LOG_DIR" \
    "$ISO/venv" \
    "$XDG_CACHE_HOME" \
    "$XDG_CONFIG_HOME" \
    "$XDG_DATA_HOME"
}

require_bootstrap_uv() {
  [[ -x "$BOOTSTRAP_UV_BIN" ]] || die "bootstrap uv not found at $BOOTSTRAP_UV_BIN"
  if [[ "$BOOTSTRAP_UV_BIN" == *"/.pyenv/shims/"* ]]; then
    die "bootstrap uv must be a real binary, not a pyenv shim: $BOOTSTRAP_UV_BIN"
  fi
}

export_isolated_env() {
  unset \
    CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_SHLVL \
    PYENV_ROOT PYENV_VERSION PYENV_DIR \
    VIRTUAL_ENV _OLD_VIRTUAL_PATH _OLD_VIRTUAL_PS1 _OLD_VIRTUAL_PROMPT \
    PYTHONPATH PYTHONHOME \
    PIP_CONFIG_FILE PIP_INDEX_URL PIP_EXTRA_INDEX_URL PIP_REQUIRE_VIRTUALENV \
    UV_INDEX UV_DEFAULT_INDEX UV_PROJECT UV_PYTHON \
    FLINK_HOME FLINK_CONF_DIR FLINK_LIB_DIR FLINK_OPT_DIR FLINK_PLUGINS_DIR

  export HOME="$ISO_HOME"
  export XDG_CACHE_HOME="$XDG_CACHE_HOME"
  export XDG_CONFIG_HOME="$XDG_CONFIG_HOME"
  export XDG_DATA_HOME="$XDG_DATA_HOME"
  export UV_CACHE_DIR="$UV_CACHE_DIR"
  export UV_PYTHON_INSTALL_DIR="$PYTHON_INSTALL_DIR"
  export UV_MANAGED_PYTHON=1
  export UV_PREFIX="$UV_HOME"
  export FLINK_UV_HOME="$UV_HOME"
  export PIP_CACHE_DIR="$PIP_CACHE_DIR"
  export TMPDIR="$TMP_DIR"
  export PYTHONNOUSERSITE=1
  export PIP_DISABLE_PIP_VERSION_CHECK=1
  export PATH="/usr/bin:/bin:/usr/sbin:/sbin"
}

download_file() {
  local url="$1"
  local dest="$2"
  /usr/bin/curl -fL "$url" -o "$dest"
}

find_jdk_home() {
  find "$JDK_DIR" -type d -path '*/Contents/Home' | head -n 1 || true
}

ensure_jdk() {
  local java_home
  java_home="$(find_jdk_home)"
  if [[ -z "$java_home" ]]; then
    msg "Downloading local JDK 17..."
    download_file "$JDK_URL" "$JDK_ARCHIVE"
    /usr/bin/tar -xzf "$JDK_ARCHIVE" -C "$JDK_DIR"
    java_home="$(find_jdk_home)"
  fi
  [[ -n "$java_home" ]] || die "failed to locate extracted JDK under $JDK_DIR"
  JAVA_HOME_LOCAL="$java_home"
}

ensure_uv_home() {
  if [[ ! -x "$UV_HOME/bin/pip" || ! -x "$UV_HOME/bin/python" || ! -x "$UV_HOME/bin/uv" ]]; then
    msg "Bootstrapping project-local uv toolchain..."
    "$BOOTSTRAP_UV_BIN" venv "$UV_HOME" --python "$PYTHON_VERSION" --seed --allow-existing
    install -m 755 "$BOOTSTRAP_UV_BIN" "$UV_HOME/bin/uv"
  fi
}

ensure_python() {
  msg "Installing managed Python $PYTHON_VERSION..."
  "$UV_BIN" python install "$PYTHON_VERSION"
  PYTHON_BIN_LOCAL="$("$UV_BIN" python find --no-project --managed-python "$PYTHON_VERSION")"
  [[ -x "$PYTHON_BIN_LOCAL" ]] || die "failed to locate managed Python $PYTHON_VERSION"
}

remove_legacy_source_override() {
  rm -f "$LEGACY_SOURCE_PTH"
}

ensure_venv() {
  if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    msg "Creating project-local Python venv..."
    "$UV_BIN" venv "$VENV_DIR" --python "$PYTHON_BIN_LOCAL" --seed --allow-existing
  fi

  remove_legacy_source_override

  if ! "$VENV_DIR/bin/python" -c 'import pyflink' >/dev/null 2>&1; then
    msg "Installing official PyFlink package from PyPI..."
    "$UV_BIN" pip install --python "$VENV_DIR/bin/python" "$PYFLINK_PYPI_SPEC"
  fi

  "$VENV_DIR/bin/python" -c 'import pyflink' >/dev/null 2>&1 \
    || die "PyFlink import failed after installation"
}

write_rcfile() {
  cat > "$RCFILE" <<EOF
export PROJECT="$PROJECT"
export ISO="$ISO"
export HOME="$ISO_HOME"
export XDG_CACHE_HOME="$XDG_CACHE_HOME"
export XDG_CONFIG_HOME="$XDG_CONFIG_HOME"
export XDG_DATA_HOME="$XDG_DATA_HOME"
export VENV_DIR="$VENV_DIR"
export UV_HOME="$UV_HOME"
export JAVA_HOME="$JAVA_HOME_LOCAL"
export UV_CACHE_DIR="$UV_CACHE_DIR"
export UV_PYTHON_INSTALL_DIR="$PYTHON_INSTALL_DIR"
export UV_MANAGED_PYTHON=1
export UV_PREFIX="$UV_HOME"
export FLINK_UV_HOME="$UV_HOME"
export PIP_CACHE_DIR="$PIP_CACHE_DIR"
export TMPDIR="$TMP_DIR"
export PYTHONNOUSERSITE=1
export PIP_DISABLE_PIP_VERSION_CHECK=1
export FLINK_LOG_DIR="$LOG_DIR"
export PYFLINK_CLIENT_EXECUTABLE="$VENV_DIR/bin/python"
export PYFLINK_PYPI_SPEC="$PYFLINK_PYPI_SPEC"
unset CONDA_PREFIX CONDA_DEFAULT_ENV CONDA_SHLVL PYENV_ROOT PYENV_VERSION PYENV_DIR VIRTUAL_ENV
unset PYTHONPATH PYTHONHOME PIP_CONFIG_FILE PIP_INDEX_URL PIP_EXTRA_INDEX_URL PIP_REQUIRE_VIRTUALENV
unset UV_INDEX UV_DEFAULT_INDEX UV_PROJECT UV_PYTHON
unset FLINK_HOME FLINK_CONF_DIR FLINK_LIB_DIR FLINK_OPT_DIR FLINK_PLUGINS_DIR
export PATH="$JAVA_HOME_LOCAL/bin:$UV_HOME/bin:$VENV_DIR/bin:/usr/bin:/bin:/usr/sbin:/sbin"
EOF

  cat >> "$RCFILE" <<EOF

pyflink_origin() {
  python - <<'PY'
import pyflink
print(pyflink.__file__)
print(getattr(pyflink, "__version__", "<unknown>"))
PY
}

source "$VENV_DIR/bin/activate"
cd "$PROJECT"
export PS1='(flink-iso) \u@\h:\w\$ '

echo 'Entered isolated Flink shell.'
echo "PROJECT=$PROJECT"
echo "HOME=$ISO_HOME"
echo "JAVA_HOME=$JAVA_HOME_LOCAL"
echo "PYTHON=$VENV_DIR/bin/python"
echo "UV=$UV_HOME/bin/uv"
echo "PYFLINK_PACKAGE=$PYFLINK_PYPI_SPEC"
echo 'PyFlink follows the official local-mode path; no FLINK_HOME is set by default.'
echo 'Run pyflink_origin to verify which package is active.'
EOF
}

enter_shell() {
  env -i \
    HOME="$ISO_HOME" \
    USER="${USER:-user}" \
    TERM="${TERM:-xterm-256color}" \
    LANG="${LANG:-en_US.UTF-8}" \
    SHELL="/bin/bash" \
    PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
    /bin/bash --noprofile --rcfile "$RCFILE" -i
}

main() {
  ensure_dirs
  require_bootstrap_uv
  export_isolated_env
  ensure_uv_home
  ensure_python
  ensure_jdk
  ensure_venv
  write_rcfile
  enter_shell
}

main "$@"
