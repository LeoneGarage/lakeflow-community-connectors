#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../../../../../.." && pwd)"
output="$repo_root/src/databricks/labs/community_connector/sources/informix/_generated_informix_python_source.py"

python3 "$repo_root/tools/scripts/merge_python_source.py" informix -o "$output"
