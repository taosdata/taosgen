#!/usr/bin/env bash

# Copyright (c) 2026 TAOS Data, Inc.
#
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TAOSGEN_ROOT=$(cd "${SCRIPT_DIR}/../../.." && pwd)
BUILD_TYPE=${BUILD_TYPE:-Release}
BUILD_TYPE_LOWER=$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')
BUILD_DIR=${BUILD_DIR:-"${SCRIPT_DIR}/build-${BUILD_TYPE_LOWER}"}

if command -v nproc >/dev/null 2>&1; then
    JOBS=${JOBS:-$(nproc)}
elif command -v sysctl >/dev/null 2>&1; then
    JOBS=${JOBS:-$(sysctl -n hw.ncpu)}
else
    JOBS=${JOBS:-2}
fi

mkdir -p "${BUILD_DIR}"

if command -v conan >/dev/null 2>&1; then
  conan install "${TAOSGEN_ROOT}" \
    --build=missing \
    --output-folder="${BUILD_DIR}/conan" \
    --settings=build_type="${BUILD_TYPE}"
fi

CMAKE_ARGS=(
  -S "${SCRIPT_DIR}"
  -B "${BUILD_DIR}"
  -DCMAKE_BUILD_TYPE="${BUILD_TYPE}"
)

if [[ -f "${BUILD_DIR}/conan/conan_toolchain.cmake" ]]; then
  CMAKE_ARGS+=(
    -DCMAKE_TOOLCHAIN_FILE="${BUILD_DIR}/conan/conan_toolchain.cmake"
    -DCMAKE_PREFIX_PATH="${BUILD_DIR}/conan"
  )
fi

cmake "${CMAKE_ARGS[@]}"
cmake --build "${BUILD_DIR}" -j "${JOBS}"

echo "Built: ${BUILD_DIR}/taosgen_ingress_cpp_client"
