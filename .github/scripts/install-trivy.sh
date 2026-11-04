#!/usr/bin/env bash
set -euo pipefail

# Use the official release asset directly; the upstream installer routes the
# archive through get.trivy.dev, whose DNS failure can prevent all image checks.
version=0.72.0
case "$(uname -sm)" in
  'Linux x86_64')
    architecture=64bit
    checksum=bbb64b9695866ce4a7a8f5c9592002c5961cab378577fa3f8a040df362b9b2ea
    ;;
  'Linux aarch64')
    architecture=ARM64
    checksum=2ca2c023109c2db6b2b77366b6717291452d4531167377d95c79547f0c8e3467
    ;;
  *) echo 'Unsupported Trivy runner platform' >&2; exit 1 ;;
esac

destination="${RUNNER_TEMP:?}/cpr-trivy-${version}"
temporary="$(mktemp -d "${RUNNER_TEMP}/cpr-trivy-download.XXXXXX")"
trap 'rm -rf "$temporary"' EXIT
archive="trivy_${version}_Linux-${architecture}.tar.gz"
curl --fail --show-error --location --retry 3 --retry-all-errors \
  --connect-timeout 15 --max-time 180 \
  "https://github.com/aquasecurity/trivy/releases/download/v${version}/${archive}" \
  --output "$temporary/$archive"
printf '%s  %s\n' "$checksum" "$temporary/$archive" | sha256sum --check --strict
tar -xzf "$temporary/$archive" -C "$temporary" trivy
install -d "$destination"
install -m 0755 "$temporary/trivy" "$destination/trivy"
"$destination/trivy" --version
printf '%s\n' "$destination" >> "${GITHUB_PATH:?}"
