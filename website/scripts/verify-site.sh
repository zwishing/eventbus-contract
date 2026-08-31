#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
site_dir="${1:-$(cd "${script_dir}/.." && pwd)}"
public_dir="${site_dir}/public"

fail() {
  printf 'verify-site: %s\n' "$*" >&2
  exit 1
}

require_file() {
  [[ -f "$1" ]] || fail "missing file: $1"
}

require_text() {
  local file="$1"
  local text="$2"
  require_file "${file}"
  grep -Fq -- "${text}" "${file}" || fail "missing text '${text}' in ${file}"
}

require_glob() {
  compgen -G "$1" >/dev/null || fail "missing generated file matching: $1"
}

require_file "${site_dir}/hugo.yaml"
require_file "${site_dir}/go.mod"
require_file "${site_dir}/go.sum"
require_file "${site_dir}/content/_index.md"
require_file "${site_dir}/content/docs/_index.md"
require_text "${site_dir}/hugo.yaml" "min: 0.165.0"
require_file "${public_dir}/index.html"
require_file "${public_dir}/index.md"
require_file "${public_dir}/index.xml"
require_file "${public_dir}/llms.txt"
require_file "${public_dir}/sitemap.xml"
require_file "${public_dir}/robots.txt"
require_file "${public_dir}/404.html"
require_file "${public_dir}/docs/index.xml"
require_file "${public_dir}/_print/docs/index.html"
require_glob "${public_dir}/offline-search-index.*.json"
require_text "${public_dir}/index.html" "eventbus-contract"
require_text "${public_dir}/index.html" "/eventbus-contract/"
require_text "${public_dir}/index.html" "https://mapseekai.github.io/eventbus-contract/"
require_text "${public_dir}/index.html" "offline-search-index"
require_text "${public_dir}/llms.txt" "文档"
require_text "${public_dir}/robots.txt" "Allow: /"
require_text "${public_dir}/robots.txt" "Sitemap:"

printf 'verify-site: baseline contract passed\n'
