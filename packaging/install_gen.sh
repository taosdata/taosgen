#!/bin/bash

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

set -e

silent_mode=0
while getopts "sh" arg; do
  case $arg in
    s) silent_mode=1 ;;
    h)
      echo "Usage: $(basename "$0") [-s]"
      echo "  -s  Silent install"
      exit 0
      ;;
    ?)
      echo "Unknown argument. Use -h for help."
      exit 1
      ;;
  esac
done

script_dir="$(dirname "$(readlink -f "$0")")"
package_tar="${script_dir}/package.tar.gz"
install_bin_dir="/usr/bin"
binary_name="taosgen"

if [ ! -f "$package_tar" ]; then
  echo "Error: package.tar.gz not found: $package_tar"
  exit 1
fi

tmp_extract_dir="${script_dir}/.gen-package"
rm -rf "$tmp_extract_dir"
mkdir -p "$tmp_extract_dir"
tar -zxf "$package_tar" -C "$tmp_extract_dir"

if [ ! -f "${tmp_extract_dir}/bin/${binary_name}" ]; then
  echo "Error: packaged binary not found: ${tmp_extract_dir}/bin/${binary_name}"
  rm -rf "$tmp_extract_dir"
  exit 1
fi

install -m 755 "${tmp_extract_dir}/bin/${binary_name}" "${install_bin_dir}/${binary_name}"
rm -rf "$tmp_extract_dir"

if ! ldconfig -p 2>/dev/null | grep -q 'libtaos\.so'; then
  found_libtaos=0
  for candidate in /usr/lib/libtaos.so /usr/lib64/libtaos.so /usr/local/lib/libtaos.so /usr/local/taos/driver/libtaos.so*; do
    if compgen -G "$candidate" > /dev/null; then
      found_libtaos=1
      break
    fi
  done
  if [ "$found_libtaos" -eq 0 ]; then
    echo "Warning: libtaos.so not found in common library paths. Install taos-community first so taosgen can run." >&2
  fi
fi

if [ "$silent_mode" -eq 0 ]; then
  echo "Installed to ${install_bin_dir}/${binary_name}"
fi
echo "taosgen installed successfully"
